//! Port of `src/backend/utils/activity/backend_progress.c` (PostgreSQL 18.3):
//! command progress reporting infrastructure.
//!
//! Each entry point operates on PostgreSQL's own backend status entry
//! (`MyBEEntry`, owned by `backend_status.c`) through the
//! `backend-utils-activity-status-seams` `with_my_beentry` slot; the
//! `PGSTAT_BEGIN_WRITE_ACTIVITY` / `PGSTAT_END_WRITE_ACTIVITY` bracketing and
//! the field writes — the logic this file owns — stay here. `IsParallelWorker()`
//! is reached through its owner's seam crate; the libpq message helpers are a
//! direct (acyclic) dependency on `backend-libpq-pqformat`.

use core::sync::atomic::{fence, AtomicU32, Ordering};

use backend_access_transam_parallel_seams::is_parallel_worker;
use backend_libpq_pqformat::{pq_beginmessage, pq_endmessage, pq_sendint32, pq_sendint64};
use backend_utils_activity_status_seams::{my_be_entry_present, track_activities, with_my_beentry};
use mcx::Mcx;
use types_core::{int64, InvalidOid, Oid};
use types_error::PgResult;
use types_pgstat::backend_progress::ProgressCommandType;
use types_pgstat::backend_status::PgBackendStatus;

pub use types_pgstat::backend_progress::PGSTAT_NUM_PROGRESS_PARAM;

/// `PqMsg_Progress` — `'P'` (`libpq/protocol.h`).
pub const PQ_MSG_PROGRESS: u8 = b'P';

/// `PGSTAT_BEGIN_WRITE_ACTIVITY(beentry)` (`utils/backend_status.h`):
/// `START_CRIT_SECTION(); st_changecount++; pg_write_barrier();`
///
/// Same barrier mapping (and the same critical-section elision) as the pgstat
/// changecount protocol — see `changecount.rs`.
fn pgstat_begin_write_activity(cc: &AtomicU32) {
    let before = cc.load(Ordering::Relaxed);
    cc.store(before.wrapping_add(1), Ordering::Relaxed);
    fence(Ordering::Release);
}

/// `PGSTAT_END_WRITE_ACTIVITY(beentry)` (`utils/backend_status.h`):
/// `pg_write_barrier(); st_changecount++;
/// Assert((st_changecount & 1) == 0); END_CRIT_SECTION();`
fn pgstat_end_write_activity(cc: &AtomicU32) {
    let before = cc.load(Ordering::Relaxed);
    cc.store(before.wrapping_add(1), Ordering::Release);
    debug_assert!((cc.load(Ordering::Relaxed) & 1) == 0);
}

/// `pgstat_progress_start_command()` —
///
/// Set `st_progress_command` (and `st_progress_command_target`) in own backend
/// entry.  Also, zero-initialize `st_progress_param` array.
pub fn pgstat_progress_start_command(cmdtype: ProgressCommandType, relid: Oid) {
    // if (!beentry || !pgstat_track_activities) return;
    if !my_be_entry_present::call() || !track_activities::call() {
        return;
    }

    with_my_beentry::call(&mut |beentry: &mut PgBackendStatus| {
        pgstat_begin_write_activity(&beentry.st_changecount);
        beentry.st_progress_command = cmdtype;
        beentry.st_progress_command_target = relid;
        // MemSet(&beentry->st_progress_param, 0, sizeof(beentry->st_progress_param));
        beentry.st_progress_param = [0; PGSTAT_NUM_PROGRESS_PARAM];
        pgstat_end_write_activity(&beentry.st_changecount);
    });
}

/// `pgstat_progress_update_param()` —
///
/// Update `index`'th member in `st_progress_param[]` of own backend entry.
pub fn pgstat_progress_update_param(index: i32, val: int64) {
    debug_assert!(index >= 0 && (index as usize) < PGSTAT_NUM_PROGRESS_PARAM);

    if !my_be_entry_present::call() || !track_activities::call() {
        return;
    }

    with_my_beentry::call(&mut |beentry: &mut PgBackendStatus| {
        pgstat_begin_write_activity(&beentry.st_changecount);
        beentry.st_progress_param[index as usize] = val;
        pgstat_end_write_activity(&beentry.st_changecount);
    });
}

/// `pgstat_progress_incr_param()` —
///
/// Increment `index`'th member in `st_progress_param[]` of own backend entry.
pub fn pgstat_progress_incr_param(index: i32, incr: int64) {
    debug_assert!(index >= 0 && (index as usize) < PGSTAT_NUM_PROGRESS_PARAM);

    if !my_be_entry_present::call() || !track_activities::call() {
        return;
    }

    with_my_beentry::call(&mut |beentry: &mut PgBackendStatus| {
        pgstat_begin_write_activity(&beentry.st_changecount);
        beentry.st_progress_param[index as usize] += incr;
        pgstat_end_write_activity(&beentry.st_changecount);
    });
}

/// `pgstat_progress_parallel_incr_param()` —
///
/// A variant of [`pgstat_progress_incr_param`] to allow a worker to poke at
/// a leader to do an incremental progress update. `Err` carries the libpq
/// message-build `ereport(ERROR)`s (StringInfo growth) and any
/// `ereport(ERROR)` under `pq_putmessage` (C discards only its EOF result).
/// The message buffer is allocated in `mcx` (C: `initStringInfo` in
/// `CurrentMemoryContext`).
pub fn pgstat_progress_parallel_incr_param(mcx: Mcx<'_>, index: i32, incr: int64) -> PgResult<()> {
    // Parallel workers notify a leader through a PqMsg_Progress message to
    // update progress, passing the progress index and incremented value.
    // Leaders can just call pgstat_progress_incr_param directly.
    if is_parallel_worker::call() {
        let mut msgbuf = pq_beginmessage(mcx, PQ_MSG_PROGRESS)?;
        pq_sendint32(&mut msgbuf, index as u32)?;
        pq_sendint64(&mut msgbuf, incr as u64)?;
        pq_endmessage(msgbuf)?;
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

    with_my_beentry::call(&mut |beentry: &mut PgBackendStatus| {
        pgstat_begin_write_activity(&beentry.st_changecount);

        for i in 0..nparam {
            debug_assert!(index[i] >= 0 && (index[i] as usize) < PGSTAT_NUM_PROGRESS_PARAM);

            // beentry->st_progress_param[index[i]] = val[i];
            beentry.st_progress_param[index[i] as usize] = val[i];
        }

        pgstat_end_write_activity(&beentry.st_changecount);
    });
}

/// `pgstat_progress_end_command()` —
///
/// Reset `st_progress_command` (and `st_progress_command_target`) in own
/// backend entry.  This signals the end of the command.
pub fn pgstat_progress_end_command() {
    if !my_be_entry_present::call() || !track_activities::call() {
        return;
    }

    with_my_beentry::call(&mut |beentry: &mut PgBackendStatus| {
        if beentry.st_progress_command == ProgressCommandType::Invalid {
            return;
        }

        pgstat_begin_write_activity(&beentry.st_changecount);
        beentry.st_progress_command = ProgressCommandType::Invalid;
        beentry.st_progress_command_target = InvalidOid;
        pgstat_end_write_activity(&beentry.st_changecount);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_seams::{with_fixture, with_flags};
    use core::sync::atomic::Ordering;

    fn changecount(e: &crate::test_seams::Env) -> u32 {
        e.beentry.borrow().st_changecount.load(Ordering::Relaxed)
    }

    #[test]
    fn start_command_noop_without_entry() {
        // !beentry => return: nothing recorded.
        with_flags(false, true, false, || {
            pgstat_progress_start_command(ProgressCommandType::Vacuum, 1);
            with_fixture(|e| {
                assert_eq!(
                    e.beentry.borrow().st_progress_command,
                    ProgressCommandType::Invalid
                );
                assert_eq!(changecount(e), 0);
            });
        });
    }

    #[test]
    fn start_command_noop_when_tracking_off() {
        with_flags(true, false, false, || {
            pgstat_progress_start_command(ProgressCommandType::Vacuum, 1);
            with_fixture(|e| {
                assert_eq!(
                    e.beentry.borrow().st_progress_command,
                    ProgressCommandType::Invalid
                );
                assert_eq!(changecount(e), 0);
            });
        });
    }

    #[test]
    fn start_then_update_incr_multi_then_end() {
        with_flags(true, true, false, || {
            pgstat_progress_start_command(ProgressCommandType::Vacuum, 1234);
            with_fixture(|e| {
                let b = e.beentry.borrow();
                assert_eq!(b.st_progress_command, ProgressCommandType::Vacuum);
                assert_eq!(b.st_progress_command_target, 1234);
                drop(b);
                assert_eq!(changecount(e), 2);
            });

            pgstat_progress_update_param(3, 7);
            with_fixture(|e| assert_eq!(e.beentry.borrow().st_progress_param[3], 7));

            pgstat_progress_incr_param(3, 5);
            with_fixture(|e| assert_eq!(e.beentry.borrow().st_progress_param[3], 12));

            pgstat_progress_update_multi_param(&[1, 2], &[10, 20]);
            with_fixture(|e| {
                let b = e.beentry.borrow();
                assert_eq!(b.st_progress_param[1], 10);
                assert_eq!(b.st_progress_param[2], 20);
            });

            pgstat_progress_end_command();
            with_fixture(|e| {
                let b = e.beentry.borrow();
                assert_eq!(b.st_progress_command, ProgressCommandType::Invalid);
                assert_eq!(b.st_progress_command_target, InvalidOid);
            });
        });
    }

    #[test]
    fn end_command_skips_when_already_invalid() {
        // command starts Invalid; end_command must return before any write.
        with_flags(true, true, false, || {
            pgstat_progress_end_command();
            with_fixture(|e| assert_eq!(changecount(e), 0));
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
                assert_eq!(e.beentry.borrow().st_progress_param[2], 0);
                assert_eq!(changecount(e), 0);
            });
        });
    }

    #[test]
    fn parallel_incr_param_updates_directly_in_leader() {
        with_flags(true, true, false, || {
            let ctx = mcx::MemoryContext::new("test");
            pgstat_progress_parallel_incr_param(ctx.mcx(), 2, 99).unwrap();
            with_fixture(|e| {
                assert_eq!(e.beentry.borrow().st_progress_param[2], 99);
                assert_eq!(changecount(e), 2);
                assert_eq!(e.sent.get(), None);
            });
        });
    }

    #[test]
    fn multi_param_noop_when_empty() {
        // nparam == 0 => return without write activity.
        with_flags(true, true, false, || {
            pgstat_progress_update_multi_param(&[], &[]);
            with_fixture(|e| assert_eq!(changecount(e), 0));
        });
    }
}
