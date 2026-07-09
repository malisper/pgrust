// storage_xlog.h + storage.c smgr_redo.
use types_core::{BlockNumber, ForkNumber, InvalidBlockNumber, INVALID_PROC_NUMBER, MAX_FORKNUM};
use types_error::PgResult;
use types_storage::{RelFileLocator, RelFileLocatorBackend};
use xlogreader_seams::XLogReaderState;

pub const XLOG_SMGR_CREATE: u8 = 0x10;
pub const XLOG_SMGR_TRUNCATE: u8 = 0x20;

pub const SMGR_TRUNCATE_HEAP: u32 = 0x0001;
pub const SMGR_TRUNCATE_VM: u32 = 0x0002;
pub const SMGR_TRUNCATE_FSM: u32 = 0x0004;
pub const SMGR_TRUNCATE_ALL: u32 = SMGR_TRUNCATE_HEAP | SMGR_TRUNCATE_VM | SMGR_TRUNCATE_FSM;

pub fn smgr_redo(record: &mut XLogReaderState) -> PgResult<()> {
    const XLR_INFO_MASK: u8 = 0x0F;
    let lsn = record.EndRecPtr;
    let rec = record.record.as_ref().expect("smgr redo with no decoded record");
    let info = rec.xl_info & !XLR_INFO_MASK;
    // SAFETY: points into the reader's decode buffer, valid for this callback.
    let xlrec = unsafe { rec.main_data_bytes() };
    if info == XLOG_SMGR_CREATE {
        let locator = RelFileLocator::new(
            u32::from_ne_bytes(xlrec[0..4].try_into().unwrap()),
            u32::from_ne_bytes(xlrec[4..8].try_into().unwrap()),
            u32::from_ne_bytes(xlrec[8..12].try_into().unwrap()),
        );
        let fork_num =
            ForkNumber::from_i32(i32::from_ne_bytes(xlrec[12..16].try_into().unwrap()))
                .expect("invalid forknum in XLOG_SMGR_CREATE");
        let key = RelFileLocatorBackend { locator, backend: INVALID_PROC_NUMBER };
        smgr::smgropen(locator, INVALID_PROC_NUMBER)?;
        smgr::smgrcreate(key, fork_num, true)?;
        Ok(())
    } else if info == XLOG_SMGR_TRUNCATE {
        let blkno = BlockNumber::from_ne_bytes(xlrec[0..4].try_into().unwrap());
        let locator = RelFileLocator::new(
            u32::from_ne_bytes(xlrec[4..8].try_into().unwrap()),
            u32::from_ne_bytes(xlrec[8..12].try_into().unwrap()),
            u32::from_ne_bytes(xlrec[12..16].try_into().unwrap()),
        );
        let flags = u32::from_ne_bytes(xlrec[16..20].try_into().unwrap());
        let key = RelFileLocatorBackend { locator, backend: INVALID_PROC_NUMBER };
        smgr::smgropen(locator, INVALID_PROC_NUMBER)?;

        // Recreate if dropped later in the WAL sequence; flush so the minimum
        // recovery point covers this record before the irreversible truncate.
        smgr::smgrcreate(key, ForkNumber::MAIN_FORKNUM, true)?;
        transam_xlog::XLogFlush(lsn)?;

        const NF: usize = MAX_FORKNUM as usize;
        let mut forks = [ForkNumber::MAIN_FORKNUM; NF];
        let mut old_blocks = [InvalidBlockNumber; NF];
        let mut blocks = [InvalidBlockNumber; NF];
        let mut nforks = 0;

        if flags & SMGR_TRUNCATE_HEAP != 0 {
            old_blocks[nforks] = smgr::smgrnblocks(key, ForkNumber::MAIN_FORKNUM)?;
            blocks[nforks] = blkno;
            nforks += 1;
            xlogutils::XLogTruncateRelation(locator, ForkNumber::MAIN_FORKNUM, blkno)?;
        }

        let fakerel = xlogutils::CreateFakeRelcacheEntry(locator);
        let mut need_fsm_vacuum = false;

        if flags & SMGR_TRUNCATE_FSM != 0 && smgr::smgrexists(key, ForkNumber::FSM_FORKNUM)? {
            let b = freespace::FreeSpaceMapPrepareTruncateRel(&fakerel, blkno)?;
            if b != InvalidBlockNumber {
                forks[nforks] = ForkNumber::FSM_FORKNUM;
                old_blocks[nforks] = smgr::smgrnblocks(key, ForkNumber::FSM_FORKNUM)?;
                blocks[nforks] = b;
                nforks += 1;
                need_fsm_vacuum = true;
            }
        }
        if flags & SMGR_TRUNCATE_VM != 0
            && smgr::smgrexists(key, ForkNumber::VISIBILITYMAP_FORKNUM)?
        {
            let b = visibilitymap::visibilitymap_prepare_truncate(&fakerel, blkno)?;
            if b != InvalidBlockNumber {
                forks[nforks] = ForkNumber::VISIBILITYMAP_FORKNUM;
                old_blocks[nforks] = smgr::smgrnblocks(key, ForkNumber::VISIBILITYMAP_FORKNUM)?;
                blocks[nforks] = b;
                nforks += 1;
            }
        }

        if nforks > 0 {
            init_small::globals::StartCriticalSection();
            smgr::smgrtruncate(key, &forks[..nforks], &old_blocks[..nforks], &blocks[..nforks])?;
            init_small::globals::EndCriticalSection();
        }

        // Truncated-away pages were likely marked all-free in the upper FSM
        // levels and would be preferentially handed out (storage.c).
        if need_fsm_vacuum {
            freespace::FreeSpaceMapVacuumRange(&fakerel, blkno, InvalidBlockNumber)?;
        }
        xlogutils::FreeFakeRelcacheEntry(fakerel);
        Ok(())
    } else {
        panic!("smgr_redo: unknown op code {info}");
    }
}
