//! Port of `src/backend/utils/activity/backend_progress.c` (PostgreSQL 18.3):
//! command progress reporting infrastructure.
//!
//! Each entry point operates on PostgreSQL's own backend status entry
//! (`MyBEEntry`, owned by `backend_status.c`) through the
//! `backend-utils-activity-status-seams` slots; the write *order* between the
//! `PGSTAT_BEGIN_WRITE_ACTIVITY` / `PGSTAT_END_WRITE_ACTIVITY` bracketing —
//! the logic this file owns — stays here. `IsParallelWorker()` and the libpq
//! message helpers are reached through their owners' seam crates.

use backend_access_transam_parallel_seams::is_parallel_worker;
use backend_libpq_pqformat_seams::{pq_beginmessage, pq_endmessage, pq_sendint32, pq_sendint64};
use mcx::Mcx;
use backend_utils_activity_status_seams::{
    begin_write_activity, end_write_activity, incr_progress_param, my_be_entry_present,
    progress_command, set_progress_command, set_progress_command_target, set_progress_param,
    track_activities, zero_progress_param,
};
use types_core::{int64, InvalidOid, Oid};
use types_error::PgResult;
use types_pgstat::backend_progress::ProgressCommandType;

/// `#define PGSTAT_NUM_PROGRESS_PARAM 20` (`utils/backend_progress.h`).
pub const PGSTAT_NUM_PROGRESS_PARAM: usize = 20;

/// `PqMsg_Progress` — `'P'` (`libpq/protocol.h`).
pub const PQ_MSG_PROGRESS: u8 = b'P';

/// `pgstat_progress_start_command()` —
///
/// Set `st_progress_command` (and `st_progress_command_target`) in own backend
/// entry.  Also, zero-initialize `st_progress_param` array.
pub fn pgstat_progress_start_command(cmdtype: ProgressCommandType, relid: Oid) {
    // if (!beentry || !pgstat_track_activities) return;
    if !my_be_entry_present::call() || !track_activities::call() {
        return;
    }

    begin_write_activity::call();
    set_progress_command::call(cmdtype);
    set_progress_command_target::call(relid);
    // MemSet(&beentry->st_progress_param, 0, sizeof(beentry->st_progress_param));
    zero_progress_param::call();
    end_write_activity::call();
}

/// `pgstat_progress_update_param()` —
///
/// Update `index`'th member in `st_progress_param[]` of own backend entry.
pub fn pgstat_progress_update_param(index: i32, val: int64) {
    debug_assert!(index >= 0 && (index as usize) < PGSTAT_NUM_PROGRESS_PARAM);

    if !my_be_entry_present::call() || !track_activities::call() {
        return;
    }

    begin_write_activity::call();
    set_progress_param::call(index, val);
    end_write_activity::call();
}

/// `pgstat_progress_incr_param()` —
///
/// Increment `index`'th member in `st_progress_param[]` of own backend entry.
pub fn pgstat_progress_incr_param(index: i32, incr: int64) {
    debug_assert!(index >= 0 && (index as usize) < PGSTAT_NUM_PROGRESS_PARAM);

    if !my_be_entry_present::call() || !track_activities::call() {
        return;
    }

    begin_write_activity::call();
    incr_progress_param::call(index, incr);
    end_write_activity::call();
}

/// `pgstat_progress_parallel_incr_param()` —
///
/// A variant of [`pgstat_progress_incr_param`] to allow a worker to poke at
/// a leader to do an incremental progress update. `Err` carries the libpq
/// message-build `ereport(ERROR)`s (StringInfo growth). The message buffer
/// is allocated in `mcx` (C: `initStringInfo` in `CurrentMemoryContext`).
pub fn pgstat_progress_parallel_incr_param(mcx: Mcx<'_>, index: i32, incr: int64) -> PgResult<()> {
    // Parallel workers notify a leader through a PqMsg_Progress message to
    // update progress, passing the progress index and incremented value.
    // Leaders can just call pgstat_progress_incr_param directly.
    if is_parallel_worker::call() {
        let mut msgbuf = pq_beginmessage::call(mcx, PQ_MSG_PROGRESS)?;
        pq_sendint32::call(&mut msgbuf, index as u32)?;
        pq_sendint64::call(&mut msgbuf, incr as u64)?;
        pq_endmessage::call(msgbuf);
    } else {
        pgstat_progress_incr_param(index, incr);
    }
    Ok(())
}

/// `pgstat_progress_update_multi_param()` —
///
/// Update multiple members in `st_progress_param[]` of own backend entry.
/// This is atomic; readers won't see intermediate states.
///
/// In C the parameters are `(int nparam, const int *index, const int64 *val)`;
/// here `index` and `val` are slices with `nparam == index.len()`.
pub fn pgstat_progress_update_multi_param(index: &[i32], val: &[int64]) {
    let nparam = index.len();

    if !my_be_entry_present::call() || !track_activities::call() || nparam == 0 {
        return;
    }

    begin_write_activity::call();

    for i in 0..nparam {
        debug_assert!(index[i] >= 0 && (index[i] as usize) < PGSTAT_NUM_PROGRESS_PARAM);

        // beentry->st_progress_param[index[i]] = val[i];
        set_progress_param::call(index[i], val[i]);
    }

    end_write_activity::call();
}

/// `pgstat_progress_end_command()` —
///
/// Reset `st_progress_command` (and `st_progress_command_target`) in own
/// backend entry.  This signals the end of the command.
pub fn pgstat_progress_end_command() {
    if !my_be_entry_present::call() || !track_activities::call() {
        return;
    }

    if progress_command::call() == ProgressCommandType::Invalid {
        return;
    }

    begin_write_activity::call();
    set_progress_command::call(ProgressCommandType::Invalid);
    set_progress_command_target::call(InvalidOid);
    end_write_activity::call();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_seams::{with_fixture, with_flags};

    #[test]
    fn start_command_noop_without_entry() {
        // !beentry => return: nothing recorded.
        with_flags(false, true, false, || {
            pgstat_progress_start_command(ProgressCommandType::Vacuum, 1);
            with_fixture(|e| {
                assert_eq!(e.command.get(), ProgressCommandType::Invalid);
                assert_eq!(e.changecount.get(), 0);
            });
        });
    }

    #[test]
    fn start_command_noop_when_tracking_off() {
        with_flags(true, false, false, || {
            pgstat_progress_start_command(ProgressCommandType::Vacuum, 1);
            with_fixture(|e| {
                assert_eq!(e.command.get(), ProgressCommandType::Invalid);
                assert_eq!(e.changecount.get(), 0);
            });
        });
    }

    #[test]
    fn start_then_update_incr_multi_then_end() {
        with_flags(true, true, false, || {
            pgstat_progress_start_command(ProgressCommandType::Vacuum, 1234);
            with_fixture(|e| {
                assert_eq!(e.command.get(), ProgressCommandType::Vacuum);
                assert_eq!(e.target.get(), 1234);
                assert_eq!(e.changecount.get(), 2);
            });

            pgstat_progress_update_param(3, 7);
            with_fixture(|e| assert_eq!(e.params[3].get(), 7));

            pgstat_progress_incr_param(3, 5);
            with_fixture(|e| assert_eq!(e.params[3].get(), 12));

            pgstat_progress_update_multi_param(&[1, 2], &[10, 20]);
            with_fixture(|e| {
                assert_eq!(e.params[1].get(), 10);
                assert_eq!(e.params[2].get(), 20);
            });

            pgstat_progress_end_command();
            with_fixture(|e| {
                assert_eq!(e.command.get(), ProgressCommandType::Invalid);
                assert_eq!(e.target.get(), InvalidOid);
            });
        });
    }

    #[test]
    fn end_command_skips_when_already_invalid() {
        // command starts Invalid; end_command must return before any write.
        with_flags(true, true, false, || {
            pgstat_progress_end_command();
            with_fixture(|e| assert_eq!(e.changecount.get(), 0));
        });
    }

    #[test]
    fn parallel_incr_param_sends_message_in_worker() {
        with_flags(true, true, true, || {
            let ctx = mcx::MemoryContext::new("test");
            pgstat_progress_parallel_incr_param(ctx.mcx(), 2, 99).unwrap();
            with_fixture(|e| {
                assert_eq!(e.sent.get(), Some((2, 99)));
                // The leader's progress_param must NOT have been touched.
                assert_eq!(e.params[2].get(), 0);
                assert_eq!(e.changecount.get(), 0);
            });
        });
    }

    #[test]
    fn parallel_incr_param_updates_directly_in_leader() {
        with_flags(true, true, false, || {
            let ctx = mcx::MemoryContext::new("test");
            pgstat_progress_parallel_incr_param(ctx.mcx(), 2, 99).unwrap();
            with_fixture(|e| {
                assert_eq!(e.params[2].get(), 99);
                assert_eq!(e.changecount.get(), 2);
                assert_eq!(e.sent.get(), None);
            });
        });
    }

    #[test]
    fn multi_param_noop_when_empty() {
        // nparam == 0 => return without write activity.
        with_flags(true, true, false, || {
            pgstat_progress_update_multi_param(&[], &[]);
            with_fixture(|e| assert_eq!(e.changecount.get(), 0));
        });
    }
}
