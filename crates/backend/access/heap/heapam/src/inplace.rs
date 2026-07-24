// heapam.c inplace lane. C divergence: the page mutates before XLogInsert
// (an order C's comment calls equivalent), so no DELAY_CHKPT_START/stack copy.
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

    // NO EREPORT(ERROR) till done (crit section pends miscadmin, per dml.rs).
    {
        let offnum = ItemPointerGetOffsetNumber(&tuple.t_self);
        // SAFETY: pin + exclusive content lock held since heap_inplace_lock.
        let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) };
        let lp = page.item_id(offnum);
        let (ptr, len) = page.item_raw(lp);
        debug_assert!(old_hoff as u32 + newlen as u32 <= len);
        // SAFETY: within the item's storage; sole writer under the lock.
        unsafe {
            core::ptr::copy_nonoverlapping(
                src.as_ptr(),
                ptr.cast_mut().add(old_hoff as usize),
                newlen,
            );
        }
    }

    bufmgr_seams::mark_buffer_dirty::call(buffer)?;

    if relation_needs_wal(relation) {
        let mut xlrec = [0u8; MIN_SIZE_OF_HEAP_INPLACE];
        xlrec[0..2].copy_from_slice(&ItemPointerGetOffsetNumber(&tuple.t_self).to_ne_bytes());
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

        let recptr = crate::wal::insert_record(
            RM_HEAP_ID,
            XLOG_HEAP_INPLACE,
            0,
            &[&xlrec, &msgs_b],
            &[crate::wal::reg_block(
                0,
                relation.rd_locator.get(),
                ItemPointerGetBlockNumber(&tuple.t_self),
                buffer,
                REGBUF_STANDARD,
                &[src],
            )],
        )?;
        // SAFETY: pin + exclusive content lock held.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) };
        pm.set_lsn(recptr);
    }

    bufmgr_seams::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;

    // Shared-queue invals before UnlockTuple (SearchSysCacheLocked1 assumes it).
    inval::eoxact::AtInplace_Inval()?;
    lmgr::UnlockTuple(relation, &tuple.t_self, InplaceUpdateTupleLock)?;
    inval::local::AcceptInvalidationMessages()?;

    inval::invalidate::CacheInvalidateHeapTuple(relation, tuple, None)?;
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
