//! Port of PostgreSQL's base-backup progress sink
//! (`src/backend/backup/basebackup_progress.c`, PostgreSQL 18.3).
//!
//! This [`Bbsink`] is responsible for command-progress reporting during a base
//! backup. Despite the name, it doesn't just report progress: it also updates a
//! few [`BbsinkState`] fields that other sinks consult (the running
//! bytes-done counter and the current-tablespace index). The general flow of a
//! base backup is: we add a progress sink (this file) to the head of the chain,
//! it forwards almost everything to the next sink, and it intercepts the
//! callbacks where it has something to report or a counter to bump.
//!
//! # What is ported in-crate vs. seamed
//!
//! All of the genuinely portable logic — which progress *phase* each callback
//! transitions to, the per-archive tablespace counting (one archive per
//! tablespace, with a guard so the streamed count never exceeds the total), and
//! the running bytes-done / total-bytes bookkeeping — is ported 1:1 in-crate
//! over the owned [`BbsinkOps`] trait and the owned [`BbsinkState`].
//!
//! The four `pgstat_progress_*` externals
//! (`backend/utils/activity/backend_progress.c`) are a direct, acyclic
//! dependency on `backend-utils-activity-small`, which has landed those
//! functions; the phase / byte arithmetic itself is computed here.
//!
//! Note that `list_length(state->tablespaces)` in the C code is just the length
//! of the tablespace list; here that is `state.tablespaces.len()`, a plain
//! owned-`Vec` length, so it needs no seam (the faithful C-ABI port routed it
//! through a `list_length` seam only because its tablespaces were a raw
//! `List *`).
//!
//! # The C `bbsink_progress` struct
//!
//! ```c
//! typedef struct bbsink_progress
//! {
//!     bbsink      base;
//! } bbsink_progress;
//! ```
//!
//! The C struct carries no state of its own beyond the base `bbsink` (it is
//! allocated with a bare `palloc0`). Accordingly [`BbsinkProgress`], the
//! [`BbsinkOps`] implementation installed into the surrounding [`Bbsink`], is a
//! zero-sized unit struct: the forwarding chain and working buffer are owned by
//! the [`Bbsink`] it is installed into.

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

use alloc::boxed::Box;

use ::sink::{
    bbsink_forward_archive_contents, bbsink_forward_begin_archive, bbsink_forward_begin_backup,
    bbsink_forward_begin_manifest, bbsink_forward_cleanup, bbsink_forward_end_archive,
    bbsink_forward_end_backup, bbsink_forward_end_manifest, bbsink_forward_manifest_contents,
    Bbsink, BbsinkOps, BbsinkState,
};
use ::activity_small::backend_progress::{
    pgstat_progress_end_command, pgstat_progress_start_command,
    pgstat_progress_update_multi_param, pgstat_progress_update_param,
};
use ::mcx::Mcx;
use ::types_core::primitive::{int64, InvalidOid, Size, TimeLineID, XLogRecPtr};
use ::types_error::PgResult;
use ::types_pgstat::backend_progress::ProgressCommandType;

// ---------------------------------------------------------------------------
// Constants from src/include/commands/progress.h.
//
// These are the `PROGRESS_BASEBACKUP_*` parameter (column) indexes and the
// `PROGRESS_BASEBACKUP_PHASE_*` phase values. They must match C exactly; the
// progress backend stores them verbatim into the cumulative-stats parameter
// array.
// ---------------------------------------------------------------------------

/// `PROGRESS_BASEBACKUP_PHASE` — column index of the current phase.
const PROGRESS_BASEBACKUP_PHASE: i32 = 0;
/// `PROGRESS_BASEBACKUP_BACKUP_TOTAL` — estimated total bytes to stream.
const PROGRESS_BASEBACKUP_BACKUP_TOTAL: i32 = 1;
/// `PROGRESS_BASEBACKUP_BACKUP_STREAMED` — bytes streamed so far.
const PROGRESS_BASEBACKUP_BACKUP_STREAMED: i32 = 2;
/// `PROGRESS_BASEBACKUP_TBLSPC_TOTAL` — total number of tablespaces.
const PROGRESS_BASEBACKUP_TBLSPC_TOTAL: i32 = 3;
/// `PROGRESS_BASEBACKUP_TBLSPC_STREAMED` — tablespaces streamed so far.
const PROGRESS_BASEBACKUP_TBLSPC_STREAMED: i32 = 4;

/// `PROGRESS_BASEBACKUP_PHASE_WAIT_CHECKPOINT` — waiting for the
/// start-of-backup checkpoint to complete.
const PROGRESS_BASEBACKUP_PHASE_WAIT_CHECKPOINT: int64 = 1;
/// `PROGRESS_BASEBACKUP_PHASE_ESTIMATE_BACKUP_SIZE` — estimating the total
/// backup size.
const PROGRESS_BASEBACKUP_PHASE_ESTIMATE_BACKUP_SIZE: int64 = 2;
/// `PROGRESS_BASEBACKUP_PHASE_STREAM_BACKUP` — streaming database files.
const PROGRESS_BASEBACKUP_PHASE_STREAM_BACKUP: int64 = 3;
/// `PROGRESS_BASEBACKUP_PHASE_WAIT_WAL_ARCHIVE` — waiting for WAL archiving at
/// end-of-backup.
const PROGRESS_BASEBACKUP_PHASE_WAIT_WAL_ARCHIVE: int64 = 4;
/// `PROGRESS_BASEBACKUP_PHASE_TRANSFER_WAL` — transferring WAL files into the
/// final archive.
const PROGRESS_BASEBACKUP_PHASE_TRANSFER_WAL: int64 = 5;

// ---------------------------------------------------------------------------
// The progress sink (C `bbsink_progress`).
// ---------------------------------------------------------------------------

/// The base-backup progress sink (C `bbsink_progress`).
///
/// Carries no state of its own (the C struct is a bare `bbsink`); the
/// forwarding chain and working buffer are owned by the surrounding [`Bbsink`]
/// this is installed into. Construct the sink with [`bbsink_progress_new`].
#[derive(Debug, Default, Clone, Copy)]
pub struct BbsinkProgress;

/// Create a new basebackup sink that performs progress tracking functions and
/// forwards data to a successor sink.
///
/// Mirrors C `bbsink *bbsink_progress_new(bbsink *next, bool estimate_backup_size)`.
///
/// `next` is the successor sink to which everything is forwarded; the returned
/// [`Bbsink`] wraps it (`Assert(next != NULL)` is implicit in the owned
/// `Box<Bbsink>`). As in the C code, `estimate_backup_size` is accepted but not
/// stored: the actual estimate, if any, is read from [`BbsinkState::bytes_total`]
/// in [`BbsinkProgress::begin_backup`]. `mcx` is the surrounding memory context
/// the new sink is allocated into (the C `palloc0`).
///
/// Reports that a base backup is in progress and sets the total size of the
/// backup to `-1`, which the progress backend translates to NULL. If we're
/// estimating the backup size, the real estimate is inserted later, once
/// `begin_backup` has it.
pub fn bbsink_progress_new<'mcx>(
    mcx: Mcx<'mcx>,
    next: Box<Bbsink<'mcx>>,
    _estimate_backup_size: bool,
) -> Box<Bbsink<'mcx>> {
    let sink = Box::new(Bbsink::new(mcx, Box::new(BbsinkProgress), Some(next)));

    // Report that a base backup is in progress, and set the total size of the
    // backup to -1, which will get translated to NULL. If we're estimating the
    // backup size, we'll insert the real estimate when we have it.
    pgstat_progress_start_command(ProgressCommandType::Basebackup, InvalidOid);
    pgstat_progress_update_param(PROGRESS_BASEBACKUP_BACKUP_TOTAL, -1);

    sink
}

impl<'mcx> BbsinkOps<'mcx> for BbsinkProgress {
    /// Progress reporting at start of backup (C `bbsink_progress_begin_backup`).
    ///
    /// Report that we are now streaming database files as a base backup. Also
    /// advertise the number of tablespaces, and, if known, the estimated total
    /// backup size.
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

        // Delegate to next sink.
        bbsink_forward_begin_backup(sink, state)
    }

    /// End-of-archive progress reporting (C `bbsink_progress_end_archive`).
    ///
    /// We expect one archive per tablespace, so reaching the end of an archive
    /// also means reaching the end of a tablespace. If WAL is included in the
    /// backup, we'll mark the last tablespace complete before the last archive
    /// is complete, so we need a guard here to ensure that the number of
    /// tablespaces streamed doesn't exceed the total.
    fn end_archive(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        if (state.tablespace_num as i64) < state.tablespaces.len() as i64 {
            pgstat_progress_update_param(
                PROGRESS_BASEBACKUP_TBLSPC_STREAMED,
                (state.tablespace_num + 1) as int64,
            );
        }

        // Delegate to next sink.
        bbsink_forward_end_archive(sink, state)?;

        // This is a convenient place to update the bbsink_state's notion of
        // which is the current tablespace. Note that the bbsink_state object is
        // shared across all bbsink objects involved, but we're the outermost one
        // and this is the very last thing we do.
        state.tablespace_num += 1;
        Ok(())
    }

    /// Handle progress tracking for new archive contents
    /// (C `bbsink_progress_archive_contents`).
    ///
    /// Increment the counter for the amount of data already streamed by the
    /// given number of bytes, and update the progress report for
    /// `pg_stat_progress_basebackup`.
    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        // First update bbsink_state with # of bytes done.
        state.bytes_done += len as u64;

        // Now forward to next sink.
        bbsink_forward_archive_contents(sink, state, len)?;

        // Prepare to set # of bytes done for command progress reporting.
        let index = [
            PROGRESS_BASEBACKUP_BACKUP_STREAMED,
            PROGRESS_BASEBACKUP_BACKUP_TOTAL,
        ];
        let mut val = [0_i64; 2];
        let mut nparam = 0usize;
        val[nparam] = state.bytes_done as int64;
        nparam += 1;

        // We may also want to update # of total bytes, to avoid overflowing past
        // 100% or the full size. This may make the total size number change as we
        // approach the end of the backup (the estimate will always be wrong if
        // WAL is included), but that's better than having the done column be
        // bigger than the total.
        if state.bytes_total_is_valid && state.bytes_done > state.bytes_total {
            val[nparam] = state.bytes_done as int64;
            nparam += 1;
        }

        pgstat_progress_update_multi_param(&index[..nparam], &val[..nparam]);
        Ok(())
    }

    // The remaining callbacks are pure forwards (C `bbsink_progress_ops` wires
    // these straight to the `bbsink_forward_*` helpers).

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

// ---------------------------------------------------------------------------
// Extra functions called directly by basebackup.c (not part of bbsink_ops).
// ---------------------------------------------------------------------------

/// Advertise that we are waiting for the start-of-backup checkpoint
/// (C `basebackup_progress_wait_checkpoint`).
pub fn basebackup_progress_wait_checkpoint() {
    pgstat_progress_update_param(
        PROGRESS_BASEBACKUP_PHASE,
        PROGRESS_BASEBACKUP_PHASE_WAIT_CHECKPOINT,
    );
}

/// Advertise that we are estimating the backup size
/// (C `basebackup_progress_estimate_backup_size`).
pub fn basebackup_progress_estimate_backup_size() {
    pgstat_progress_update_param(
        PROGRESS_BASEBACKUP_PHASE,
        PROGRESS_BASEBACKUP_PHASE_ESTIMATE_BACKUP_SIZE,
    );
}

/// Advertise that we are waiting for WAL archiving at end-of-backup
/// (C `basebackup_progress_wait_wal_archive`).
///
/// We report having finished all tablespaces at this point, even if the archive
/// for the main tablespace is still open, because what's going to be added is
/// WAL files, not files that are really from the main tablespace.
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

/// Advertise that we are transferring WAL files into the final archive
/// (C `basebackup_progress_transfer_wal`).
pub fn basebackup_progress_transfer_wal() {
    pgstat_progress_update_param(
        PROGRESS_BASEBACKUP_PHASE,
        PROGRESS_BASEBACKUP_PHASE_TRANSFER_WAL,
    );
}

/// Advertise that we are no longer performing a backup
/// (C `basebackup_progress_done`).
pub fn basebackup_progress_done() {
    pgstat_progress_end_command();
}

/// Install this crate's seams. Like the `backend-backup-sink` vtable leaf, the
/// progress sink exposes plain free functions to its (still unported) caller
/// `basebackup.c` and has no cross-cycle inward seams of its own, so there is
/// nothing to set; it is registered in `seams-init::init_all` for uniformity.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
