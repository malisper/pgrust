// heapam.c inplace lane.
use ::mcx::Mcx;
use ::tableam_vocab::TM_Result;
use ::types_core::{Buffer, CommandId};
use ::types_error::{PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE};
use ::types_rel::RelationData;
use ::types_storage::bufpage::{PageMut, PageRef};
use ::types_storage::lock::{InplaceUpdateTupleLock, XLTW_Oper};
use ::types_tuple::{
    HeapTupleData, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
    HEAP_XMAX_IS_KEYSHR_LOCKED, HEAP_XMAX_IS_MULTI,
};

use ::bufmgr_seams::{BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK};
use ::types_storage::{SharedInvalidationMessage, SHARED_INVALIDATION_MESSAGE_SIZE};
use ::xloginsert_seams::REGBUF_STANDARD;

use crate::dml::{relation_needs_wal, XLOG_HEAP_INPLACE};
use heapam_visibility_seams as hv_seam;

const RM_HEAP_ID: u8 = rmgr::RmgrIds::RM_HEAP_ID as u8;
// MinSizeOfHeapInplace (heapam_xlog.h).
const MIN_SIZE_OF_HEAP_INPLACE: usize = 20;

fn on_page_tuple<'any>(
    relation: &RelationData<'_>,
    buffer: Buffer,
    tid: ::types_tuple::ItemPointerData,
) -> HeapTupleData<'any> {
    // SAFETY: caller holds the pin (and a content lock) on `buffer`.
    let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) };
    let lp = page.item_id(ItemPointerGetOffsetNumber(&tid));
    debug_assert!(lp.is_normal());
    let (ptr, len) = page.item_raw(lp);
    // SAFETY: item bounds-checked against the pinned page image; the erased
    // lifetime must not outlive the pin.
    unsafe { HeapTupleData::from_raw_parts(ptr, len, tid, relation.rd_id) }
}

/// `release_callback` runs after the buffer lock drops, before the wait
/// (C's contract); an open callback set, so C's fn-pointer stays a dyn.
pub fn heap_inplace_lock(
    relation: &RelationData<'_>,
    oldtup: &HeapTupleData<'_>,
    buffer: Buffer,
    release_callback: &mut dyn FnMut() -> PgResult<()>,
) -> PgResult<bool> {
    let tid = oldtup.t_self;
    inval::invalidate::CacheInvalidateHeapTupleInplace(relation, oldtup)?;

    lmgr::LockTuple(relation, &tid, InplaceUpdateTupleLock)?;
    bufmgr_seams::lock_buffer::call(buffer, BUFFER_LOCK_EXCLUSIVE)?;

    let mut oldtup_pg = on_page_tuple(relation, buffer, tid);
    let curcid: CommandId = xact_seams::get_current_command_id::call(false)?;
    let result = hv_seam::heap_tuple_satisfies_update::call(&mut oldtup_pg, curcid, buffer)?;

    let ret: bool;
    match result {
        TM_Result::TM_Invisible => {
            return Err(Box::new(
                PgError::error("attempted to overwrite invisible tuple")
                    .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
        TM_Result::TM_SelfModified => {
            return Err(Box::new(
                PgError::error(
                    "tuple to be updated was already modified by an operation triggered by the current command",
                )
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
        TM_Result::TM_BeingModified => {
            let xwait = oldtup_pg.t_data().xmax_raw();
            let infomask = oldtup_pg.t_data().t_infomask;
            if (infomask & HEAP_XMAX_IS_MULTI) != 0 {
                use ::tableam_vocab::LockTupleMode;
                use ::types_storage::multixact::MultiXactStatus;
                let lockmode = LockTupleMode::LockTupleNoKeyExclusive;
                let mxact_status = MultiXactStatus::MultiXactStatusNoKeyUpdate;
                if crate::dml::DoesMultiXactIdConflict(xwait, infomask, lockmode, false)?.conflict {
                    bufmgr_seams::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
                    release_callback()?;
                    ret = false;
                    crate::dml::MultiXactIdWait(
                        xwait,
                        mxact_status,
                        infomask,
                        relation,
                        Some(&tid),
                        XLTW_Oper::Update,
                        None,
                    )?;
                } else {
                    ret = true;
                }
            } else if xact_seams::transaction_id_is_current_transaction_id::call(xwait)
                || HEAP_XMAX_IS_KEYSHR_LOCKED(infomask)
            {
                ret = true;
            } else {
                bufmgr_seams::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
                release_callback()?;
                ret = false;
                lmgr::XactLockTableWait(xwait, Some(relation), Some(&tid), XLTW_Oper::Update)?;
            }
        }
        other => {
            ret = other == TM_Result::TM_Ok;
            if !ret {
                bufmgr_seams::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
                release_callback()?;
            }
        }
    }

    if !ret {
        lmgr::UnlockTuple(relation, &tid, InplaceUpdateTupleLock)?;
        inval::eoxact::ForgetInplace_Inval();
        snapmgr_seams::invalidate_catalog_snapshot::call();
    }
    Ok(ret)
}

/// `oldtup` is the on-page tuple, `tuple` the same-length replacement image.
pub fn heap_inplace_update_and_unlock(
    mcx: Mcx<'_>,
    relation: &RelationData<'_>,
    oldtup: &HeapTupleData<'_>,
    tuple: &HeapTupleData<'_>,
    buffer: Buffer,
) -> PgResult<()> {
    debug_assert!(oldtup.t_self == tuple.t_self);
    let old_hoff = oldtup.t_data().t_hoff;
    let new_hoff = tuple.t_data().t_hoff;
    let oldlen = oldtup.t_len as usize - old_hoff as usize;
    let newlen = tuple.t_len as usize - new_hoff as usize;
    if oldlen != newlen || old_hoff != new_hoff {
        return Err(Box::new(PgError::error("wrong tuple length")));
    }

    // SAFETY: tuple's image is t_len readable bytes.
    let src =
        unsafe { core::slice::from_raw_parts(tuple.header_ptr().add(new_hoff as usize), newlen) };

    let (inval_messages, relcache_init_file_inval): (
        ::mcx::PgVec<'_, SharedInvalidationMessage>,
        bool,
    ) = if transam_xlog_seams::xlog_standby_info_active::call() {
        inval::eoxact::inplaceGetInvalidationMessages(mcx)?
    } else {
        (::mcx::PgVec::new_in(mcx), false)
    };
    let nmsgs = inval_messages.len() as i32;

    inval::eoxact::PreInplace_Inval()?;

    // NO EREPORT(ERROR) till changes complete: WAL goes first (a reader may
    // have pinned + visibility-checked this tuple already), then the page
    // mutates; DELAY_CHKPT_START makes XLogInsert-before-MarkBufferDirty
    // safe, as in C's XLogSaveBufferForHint. A crash between memcpy and
    // XLogInsert in the reverse order can let datfrozenxid overtake
    // relfrozenxid (heapam.c's D/R scenario).
    let my_proc = lmgr_proc::MyProc().map(lmgr_proc::GetPGProcByNumber);
    if let Some(proc) = my_proc {
        use core::sync::atomic::Ordering::Relaxed;
        use ::types_storage::storage::DELAY_CHKPT_START;
        debug_assert_eq!(proc.delayChkptFlags.load(Relaxed) & DELAY_CHKPT_START, 0);
        init_small::globals::StartCriticalSection();
        proc.delayChkptFlags.fetch_or(DELAY_CHKPT_START, Relaxed);
    } else {
        // MyProc is unset only in single-threaded unit harnesses.
        init_small::globals::StartCriticalSection();
    }

    // An Err below leaves the critical section open so the escape check
    // escalates it to PANIC, mirroring C's in-crit-section ereport promotion.
    inplace_write_wal_and_page(
        mcx,
        relation,
        tuple,
        buffer,
        src,
        old_hoff,
        newlen,
        relcache_init_file_inval,
        nmsgs,
        &inval_messages,
    )?;

    // Shared-queue invals before UnlockTuple (SearchSysCacheLocked1 assumes
    // it), still inside the critical section, as in C.
    inval::eoxact::AtInplace_Inval()?;

    if let Some(proc) = my_proc {
        use core::sync::atomic::Ordering::Relaxed;
        use ::types_storage::storage::DELAY_CHKPT_START;
        proc.delayChkptFlags.fetch_and(!DELAY_CHKPT_START, Relaxed);
    }
    init_small::globals::EndCriticalSection();

    lmgr::UnlockTuple(relation, &tuple.t_self, InplaceUpdateTupleLock)?;
    inval::local::AcceptInvalidationMessages()?;

    if !miscinit_seams::is_bootstrap_processing_mode::call() {
        inval::invalidate::CacheInvalidateHeapTuple(relation, tuple, None)?;
    }
    Ok(())
}

#[repr(align(8))]
struct AlignedBlock([u8; ::types_core::BLCKSZ]);

#[allow(clippy::too_many_arguments)]
pub(crate) fn inplace_write_wal_and_page(
    mcx: Mcx<'_>,
    relation: &RelationData<'_>,
    tuple: &HeapTupleData<'_>,
    buffer: Buffer,
    src: &[u8],
    old_hoff: u8,
    newlen: usize,
    relcache_init_file_inval: bool,
    nmsgs: i32,
    inval_messages: &::mcx::PgVec<'_, SharedInvalidationMessage>,
) -> PgResult<()> {
    let offnum = ItemPointerGetOffsetNumber(&tuple.t_self);
    // SAFETY: pin + exclusive content lock held since heap_inplace_lock.
    let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) };
    let lp = page.item_id(offnum);
    let (ptr, len) = page.item_raw(lp);
    debug_assert!(old_hoff as u32 + newlen as u32 <= len);
    let dst = unsafe { ptr.cast_mut().add(old_hoff as usize) };

    if relation_needs_wal(relation) {
        let mut xlrec = [0u8; MIN_SIZE_OF_HEAP_INPLACE];
        xlrec[0..2].copy_from_slice(&offnum.to_ne_bytes());
        xlrec[4..8].copy_from_slice(&init_small::globals::MyDatabaseId().to_ne_bytes());
        xlrec[8..12].copy_from_slice(&init_small::globals::MyDatabaseTableSpace().to_ne_bytes());
        xlrec[12] = relcache_init_file_inval as u8;
        xlrec[16..20].copy_from_slice(&nmsgs.to_ne_bytes());

        let mut msgs_b: ::mcx::PgVec<'_, u8> = ::mcx::vec_with_capacity_in(
            mcx,
            inval_messages.len() * SHARED_INVALIDATION_MESSAGE_SIZE,
        )?;
        for m in inval_messages.iter() {
            msgs_b.extend_from_slice(&m.to_wire_bytes());
        }

        // Register a stack copy of the post-mutation block (an FPI candidate
        // matching what the buffer will look like), before other sessions can
        // see the mutation; the live page is still pre-image here.
        let origdata = bufmgr_seams::buffer_get_page::call(buffer).as_ptr() as *const u8;
        let lower = page.pd_lower() as usize;
        let upper = page.pd_upper() as usize;
        let mut copied = AlignedBlock([0u8; ::types_core::BLCKSZ]);
        // SAFETY: origdata is a BLCKSZ page image under our pin + lock;
        // lower <= upper <= BLCKSZ on a valid page (REGBUF_STANDARD layout).
        unsafe {
            core::ptr::copy_nonoverlapping(origdata, copied.0.as_mut_ptr(), lower);
            core::ptr::copy_nonoverlapping(
                origdata.add(upper),
                copied.0.as_mut_ptr().add(upper),
                ::types_core::BLCKSZ - upper,
            );
        }
        let dst_offset_in_block = dst as usize - origdata as usize;
        copied.0[dst_offset_in_block..dst_offset_in_block + newlen].copy_from_slice(src);

        let recptr = crate::wal::insert_record(
            RM_HEAP_ID,
            XLOG_HEAP_INPLACE,
            0,
            &[&xlrec, &msgs_b],
            &[crate::wal::RegBlock {
                block_id: 0,
                rlocator: relation.rd_locator.get(),
                forknum: ::types_core::ForkNumber::MAIN_FORKNUM,
                block: ItemPointerGetBlockNumber(&tuple.t_self),
                page: &copied.0,
                flags: REGBUF_STANDARD,
                bufdata: &[src],
            }],
        )?;
        // SAFETY: pin + exclusive content lock held.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) };
        pm.set_lsn(recptr);
    }

    // SAFETY: within the item's storage; sole writer under the lock.
    unsafe {
        core::ptr::copy_nonoverlapping(src.as_ptr(), dst, newlen);
    }

    bufmgr_seams::mark_buffer_dirty::call(buffer)?;

    bufmgr_seams::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
    Ok(())
}

pub fn heap_inplace_unlock(
    relation: &RelationData<'_>,
    oldtup: &HeapTupleData<'_>,
    buffer: Buffer,
) -> PgResult<()> {
    bufmgr_seams::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
    lmgr::UnlockTuple(relation, &oldtup.t_self, InplaceUpdateTupleLock)?;
    inval::eoxact::ForgetInplace_Inval();
    Ok(())
}
