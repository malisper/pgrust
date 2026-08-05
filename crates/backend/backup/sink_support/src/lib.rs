//! Port of `basebackup_progress.c` (PostgreSQL 18.3): the base-backup progress
//! sink plus the `basebackup_progress_*` entry points `basebackup.c` calls
//! directly. Besides reporting, it updates the [`BbsinkState`] fields other
//! sinks consult (bytes-done, tablespace index), forwarding everything else.

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

use alloc::boxed::Box;

use ::backend_progress::{
    pgstat_progress_end_command, pgstat_progress_start_command,
    pgstat_progress_update_multi_param, pgstat_progress_update_param, PROGRESS_COMMAND_BASEBACKUP,
};
use ::mcx::Mcx;
use ::sink::{
    bbsink_forward_archive_contents, bbsink_forward_begin_archive, bbsink_forward_begin_backup,
    bbsink_forward_begin_manifest, bbsink_forward_cleanup, bbsink_forward_end_archive,
    bbsink_forward_end_backup, bbsink_forward_end_manifest, bbsink_forward_manifest_contents,
    Bbsink, BbsinkOps, BbsinkState,
};
use ::types_core::primitive::{int64, InvalidOid, Size, TimeLineID, XLogRecPtr};
use ::types_error::PgResult;

// PROGRESS_BASEBACKUP_* column indices (usize param slots) and phase values
// (i64 payloads) from src/include/commands/progress.h; must match C exactly.
const PROGRESS_BASEBACKUP_PHASE: usize = 0;
const PROGRESS_BASEBACKUP_BACKUP_TOTAL: usize = 1;
const PROGRESS_BASEBACKUP_BACKUP_STREAMED: usize = 2;
const PROGRESS_BASEBACKUP_TBLSPC_TOTAL: usize = 3;
const PROGRESS_BASEBACKUP_TBLSPC_STREAMED: usize = 4;

const PROGRESS_BASEBACKUP_PHASE_WAIT_CHECKPOINT: int64 = 1;
const PROGRESS_BASEBACKUP_PHASE_ESTIMATE_BACKUP_SIZE: int64 = 2;
const PROGRESS_BASEBACKUP_PHASE_STREAM_BACKUP: int64 = 3;
const PROGRESS_BASEBACKUP_PHASE_WAIT_WAL_ARCHIVE: int64 = 4;
const PROGRESS_BASEBACKUP_PHASE_TRANSFER_WAL: int64 = 5;

/// C `bbsink_progress` — a bare `bbsink` with no state of its own.
#[derive(Debug, Default, Clone, Copy)]
pub struct BbsinkProgress;

/// `estimate_backup_size` is accepted but not stored (as in C): the estimate is
/// read from [`BbsinkState::bytes_total`] in `begin_backup`.
pub fn bbsink_progress_new<'mcx>(
    mcx: Mcx<'mcx>,
    next: Box<Bbsink<'mcx>>,
    _estimate_backup_size: bool,
) -> Box<Bbsink<'mcx>> {
    let sink = Box::new(Bbsink::new(mcx, Box::new(BbsinkProgress), Some(next)));
    pgstat_progress_start_command(PROGRESS_COMMAND_BASEBACKUP, InvalidOid);
    pgstat_progress_update_param(PROGRESS_BASEBACKUP_BACKUP_TOTAL, -1);
    sink
}

impl<'mcx> BbsinkOps<'mcx> for BbsinkProgress {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        let total: int64 = if state.bytes_total_is_valid {
            state.bytes_total as int64
        } else {
            -1
        };
        let index = [
            PROGRESS_BASEBACKUP_PHASE,
            PROGRESS_BASEBACKUP_BACKUP_TOTAL,
            PROGRESS_BASEBACKUP_TBLSPC_TOTAL,
        ];
        let val: [int64; 3] = [
            PROGRESS_BASEBACKUP_PHASE_STREAM_BACKUP,
            total,
            state.tablespaces.len() as int64,
        ];
        pgstat_progress_update_multi_param(&index, &val);
        bbsink_forward_begin_backup(sink, state)
    }

    fn end_archive(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        // Guard: keep streamed count <= total when WAL adds a trailing archive.
        if (state.tablespace_num as i64) < state.tablespaces.len() as i64 {
            pgstat_progress_update_param(
                PROGRESS_BASEBACKUP_TBLSPC_STREAMED,
                (state.tablespace_num + 1) as int64,
            );
        }
        bbsink_forward_end_archive(sink, state)?;
        state.tablespace_num += 1;
        Ok(())
    }

    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        state.bytes_done += len as u64;
        bbsink_forward_archive_contents(sink, state, len)?;

        let index = [
            PROGRESS_BASEBACKUP_BACKUP_STREAMED,
            PROGRESS_BASEBACKUP_BACKUP_TOTAL,
        ];
        let mut val = [0_i64; 2];
        let mut nparam = 0usize;
        val[nparam] = state.bytes_done as int64;
        nparam += 1;
        // Bump the reported total past the estimate so `done` never exceeds it.
        if state.bytes_total_is_valid && state.bytes_done > state.bytes_total {
            val[nparam] = state.bytes_done as int64;
            nparam += 1;
        }
        pgstat_progress_update_multi_param(&index[..nparam], &val[..nparam]);
        Ok(())
    }

    fn begin_archive(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        archive_name: &str,
    ) -> PgResult<()> {
        bbsink_forward_begin_archive(sink, state, archive_name)
    }

    fn begin_manifest(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_begin_manifest(sink, state)
    }

    fn manifest_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        bbsink_forward_manifest_contents(sink, state, len)
    }

    fn end_manifest(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_end_manifest(sink, state)
    }

    fn end_backup(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        endptr: XLogRecPtr,
        endtli: TimeLineID,
    ) -> PgResult<()> {
        bbsink_forward_end_backup(sink, state, endptr, endtli)
    }

    fn cleanup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_cleanup(sink, state)
    }
}

pub fn basebackup_progress_wait_checkpoint() {
    pgstat_progress_update_param(
        PROGRESS_BASEBACKUP_PHASE,
        PROGRESS_BASEBACKUP_PHASE_WAIT_CHECKPOINT,
    );
}

pub fn basebackup_progress_estimate_backup_size() {
    pgstat_progress_update_param(
        PROGRESS_BASEBACKUP_PHASE,
        PROGRESS_BASEBACKUP_PHASE_ESTIMATE_BACKUP_SIZE,
    );
}

/// Reports all tablespaces done even though the main archive may still be open:
/// what follows is WAL, not tablespace files.
pub fn basebackup_progress_wait_wal_archive(state: &BbsinkState) {
    let index = [
        PROGRESS_BASEBACKUP_PHASE,
        PROGRESS_BASEBACKUP_TBLSPC_STREAMED,
    ];
    let val: [int64; 2] = [
        PROGRESS_BASEBACKUP_PHASE_WAIT_WAL_ARCHIVE,
        state.tablespaces.len() as int64,
    ];
    pgstat_progress_update_multi_param(&index, &val);
}

pub fn basebackup_progress_transfer_wal() {
    pgstat_progress_update_param(
        PROGRESS_BASEBACKUP_PHASE,
        PROGRESS_BASEBACKUP_PHASE_TRANSFER_WAL,
    );
}

pub fn basebackup_progress_done() {
    pgstat_progress_end_command();
}

pub fn init_seams() {}

#[cfg(test)]
mod tests;
