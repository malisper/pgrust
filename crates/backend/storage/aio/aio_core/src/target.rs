//! aio_target.c: the (closed) target registry. Only PGAIO_TID_SMGR exists;
//! its reopen/describe live in smgr, reached through smgr_seams (direct dep
//! would cycle: smgr -> md -> aio_core).

use types_core::{BlockNumber, ForkNumber};
use types_error::PgResult;
use types_storage::aio::{PgAioTargetData, PGAIO_TID_INVALID, PGAIO_TID_SMGR};
use types_storage::RelFileLocator;

use crate::{ioh, PGAIO_HS_HANDED_OUT};

pub(crate) fn pgaio_io_has_target(index: u32) -> bool {
    // SAFETY: owner thread pre-submission (callers are on define edges).
    unsafe { ioh(index).data() }.target != PGAIO_TID_INVALID
}

pub fn pgaio_io_set_target_smgr(
    index: u32,
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: BlockNumber,
    is_temp: bool,
    skip_fsync: bool,
) {
    let h = ioh(index);
    debug_assert!(h.state() == PGAIO_HS_HANDED_OUT);
    // SAFETY: HANDED_OUT, owner thread.
    let d = unsafe { h.data() };
    debug_assert!(d.target == PGAIO_TID_INVALID);
    d.target = PGAIO_TID_SMGR;
    d.target_data.smgr.rlocator = rlocator;
    d.target_data.smgr.forkNum = forknum;
    d.target_data.smgr.blockNum = blocknum;
    d.target_data.smgr.nblocks = nblocks;
    d.target_data.smgr.is_temp = is_temp;
    d.target_data.smgr.skip_fsync = skip_fsync;
}

pub fn pgaio_io_get_target_data(index: u32) -> PgAioTargetData {
    // SAFETY: readers are on completion edges where d is stable.
    unsafe { ioh(index).data() }.target_data
}

pub(crate) fn pgaio_io_can_reopen(index: u32) -> bool {
    // SAFETY: as pgaio_io_has_target.
    unsafe { ioh(index).data() }.target == PGAIO_TID_SMGR
}

/// Reopen in the EXECUTING thread via its own vfd cache; the new raw fd
/// lands in op_data (C parity for cross-process fd invalidity).
pub(crate) fn pgaio_io_reopen(index: u32) -> PgResult<()> {
    // SAFETY: the worker owns d between queue consume and completion.
    let d = unsafe { ioh(index).data() };
    debug_assert!(d.target == PGAIO_TID_SMGR);
    debug_assert!(d.op != types_storage::aio::PGAIO_OP_INVALID);
    let fd = smgr_seams::aio_smgr_reopen::call(
        d.target_data,
        d.op,
        pgaio_io_get_owner_procno_for_temp(index),
        d.op_data.offset,
    )?;
    d.op_data.fd = fd;
    Ok(())
}

fn pgaio_io_get_owner_procno_for_temp(index: u32) -> i32 {
    // SAFETY: as pgaio_io_reopen.
    if unsafe { ioh(index).data() }.target_data.smgr.is_temp {
        ioh(index).owner_procno
    } else {
        types_core::INVALID_PROC_NUMBER
    }
}
