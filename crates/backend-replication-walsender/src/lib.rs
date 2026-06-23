//! `backend-replication-walsender` — port of
//! `src/backend/replication/walsender.c` (the WAL sender process).
//!
//! # Structure
//!
//!  * [`core`] holds the shared owned declarations: the constants, the GUCs and
//!    exported flag globals (collected into one process-local [`core::WalSndProc`]),
//!    the `LagTracker` / `WalTimeSample` types, and the shared-memory
//!    `WalSndCtlData` array (`WalSndCtl`) owned as a real `#[repr(C)]` shmem
//!    struct.
//!  * The module files carry their assigned functions as 1:1 ports of the C
//!    control flow.
//!
//! ## Notes
//!
//!  * Process-local mutable state (the C file-static `static` family + the
//!    exported flag globals) is owned in one [`core::WalSndProc`]; no `static mut`.
//!  * The shared-memory `WalSnd` / `WalSndCtlData` array (its spinlocks /
//!    condition variables) is owned here as a real shmem struct, allocated
//!    through `ShmemInitStruct` (`backend-storage-ipc-shmem-seams`).
//!  * The in-crate computation — the lag tracker, the sleeptime / keepalive /
//!    timeout math, the main-loop and WAL-wait control flow, the
//!    physical/logical send-decision flow, the SRF per-row classification, and
//!    the `TransactionIdInRecentPast` XID arithmetic — is ported faithfully over
//!    owned values.
//!  * Genuinely-external subsystems (xlog, xlogreader/timeline, slots, logical
//!    decoding, libpq, syncrep, basebackup, the replication scanner/grammar,
//!    process/IPC/signals, transam/xact, and the utils families) are reached
//!    through their owner `-seams` crates; an unported owner panics loudly when
//!    called, which is correct.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

#[cfg(test)]
extern crate std;

// Owner-seam crate aliases (outward calls).
use backend_access_transam_varsup_seams as varsup;
use backend_access_transam_xact_seams as xact;
use backend_access_transam_xlog_seams as xlog;
use backend_access_transam_xlogrecovery_seams as xlogrecovery;
use backend_libpq_pqcomm_seams as pq;
use backend_replication_basebackup_seams as basebackup;
use backend_replication_logical_slotsync_seams as slotsync;
use backend_replication_slot_seams as slot;
use backend_replication_snapbuild_seams as snapbuild;
use backend_replication_syncrep_seams as syncrep;
use backend_replication_walreceiverfuncs_seams as walrcvfuncs;
use backend_storage_ipc_dsm_core_seams as ipc;
use backend_storage_ipc_latch_seams as latch;
use backend_storage_ipc_procarray_seams as procarray;
use backend_storage_ipc_procsignal_seams as procsignal;
use backend_storage_ipc_shmem_seams as shmem;
use backend_storage_lmgr_condition_variable_seams as condvar;
use backend_tcop_dest_seams as dest;
use backend_tcop_postgres_seams as tcop;
use backend_utils_adt_acl_seams as acl;
use backend_utils_adt_timestamp_seams as timestamp;
use backend_utils_init_small_seams as miscinit;
use backend_utils_misc_guc_file_seams as guc_file;
use backend_utils_misc_ps_status_seams as ps_status;
use backend_utils_resowner_resowner_seams as resowner;

pub mod core;

pub mod command;
pub mod init;
pub mod lag_tracker;
pub mod logical;
pub mod mainloop;
pub mod physical;
pub mod replies;
pub mod shmem_array;
pub mod start_replication;
pub mod stats;
pub mod wakeup;

// Re-export the public (non-static) walsender entry points (those declared in
// `replication/walsender.h`) so dependents call them at the crate root.
pub use crate::command::exec_replication_command;
pub use crate::init::{
    InitWalSender, WalSndErrorCleanup, WalSndSetState, WalSndShmemInit, WalSndShmemSize,
    WalSndSignals,
};
pub use crate::physical::PhysicalWakeupLogicalWalSnd;
pub use crate::replies::PhysicalReplicationSlotNewXmin;
pub use crate::start_replication::GetStandbyFlushRecPtr;
pub use crate::stats::pg_stat_get_wal_senders;
pub use crate::wakeup::{
    HandleWalSndInitStopping, WalSndInitStopping, WalSndRqstFileReload, WalSndWaitStopping,
    WalSndWakeup,
};

/// Install every seam this crate owns
/// (`crates/backend-replication-walsender-seams`).
pub fn init_seams() {
    use backend_replication_walsender_seams as ws;

    ws::am_walsender::set(|| core::proc_get(|p| p.am_walsender));
    ws::am_db_walsender::set(|| core::proc_get(|p| p.am_db_walsender));
    ws::set_am_walsender::set(|v| core::with_proc(|p| p.am_walsender = v));
    ws::set_am_db_walsender::set(|v| core::with_proc(|p| p.am_db_walsender = v));
    ws::log_replication_commands::set(|| core::proc_get(|p| p.log_replication_commands));
    ws::max_wal_senders::set(|| core::proc_get(|p| p.max_wal_senders));

    // `int max_wal_senders` GUC backing storage (walsender.c). Read directly
    // at shmem-sizing time by WalSndShmemSize (mul_size(max_wal_senders,
    // sizeof(WalSnd))); the GUC engine seeds it from boot_val.
    {
        use backend_utils_misc_guc_tables::{vars, GucVarAccessors};
        vars::max_wal_senders.install(GucVarAccessors {
            get: || core::proc_get(|p| p.max_wal_senders),
            set: |v| core::with_proc(|p| p.max_wal_senders = v),
        });

        // `int wal_sender_timeout = 60 * 1000;` GUC (walsender.c). A plain GUC
        // global read from its slot throughout the WAL-sender loop (e.g.
        // WalSndComputeSleeptime / ProcessRepliesIfAny timeout checks); seeded
        // by the GUC engine from boot_val.
        vars::wal_sender_timeout.install(GucVarAccessors {
            get: || core::proc_get(|p| p.wal_sender_timeout),
            set: |v| core::with_proc(|p| p.wal_sender_timeout = v),
        });

        // `bool log_replication_commands = false;` GUC (walsender.c). Read from
        // its slot when deciding LOG vs DEBUG1 for a replication command.
        vars::log_replication_commands.install(GucVarAccessors {
            get: || core::proc_get(|p| p.log_replication_commands),
            set: |v| core::with_proc(|p| p.log_replication_commands = v),
        });
    }

    ws::wal_snd_set_state::set(crate::init::WalSndSetState);

    // The PostgresMain simple-Query / error-recovery entries into the WAL
    // sender (reached only on an `am_walsender` connection).
    ws::exec_replication_command::set(crate::command::exec_replication_command);
    ws::wal_snd_error_cleanup::set(crate::init::WalSndErrorCleanup);

    // The PostgresMain backend-bootstrap entries into the WAL sender
    // (`am_walsender` connection): claim the shmem slot + install handlers.
    ws::init_wal_sender::set(crate::init::InitWalSender);
    ws::wal_snd_signals::set(crate::init::WalSndSignals);

    ws::handle_wal_snd_init_stopping::set(crate::wakeup::HandleWalSndInitStopping);
    ws::wal_snd_rqst_file_reload::set(|| {
        crate::wakeup::WalSndRqstFileReload();
        Ok(())
    });
    ws::wal_snd_wakeup::set(|physical, logical| {
        crate::wakeup::WalSndWakeup(physical, logical);
        Ok(())
    });
    ws::walsnd_wakeup_if_cascading::set(crate::wakeup::WalSndWakeupIfCascading);

    ws::wal_snd_shmem_size::set(|| Ok(crate::init::WalSndShmemSize()));
    ws::wal_snd_shmem_init::set(|| {
        crate::init::WalSndShmemInit();
        Ok(())
    });

    ws::get_standby_flush_rec_ptr::set(|| {
        let mut tli: types_core::primitive::TimeLineID = 0;
        crate::start_replication::GetStandbyFlushRecPtr(&mut tli)
    });

    ws::with_wal_confirm_rcv_cv::set(|f| {
        let cv = &crate::core::wal_snd_ctl().wal_confirm_rcv_cv;
        f(cv);
    });

    ws::WaitForStandbyConfirmation::set(crate::start_replication::WaitForStandbyConfirmation);

    // The logical-decoding output-plugin write callbacks installed on a
    // LogicalDecodingContext (`ctx->prepare_write/write/update_progress`).
    ws::call_prepare_write::set(|loc, xid, last| {
        crate::logical::WalSndPrepareWrite(loc, xid, last);
        Ok(())
    });
    ws::call_write::set(|loc, xid, last| {
        crate::logical::WalSndWriteData(loc, xid, last);
        Ok(())
    });
    ws::call_update_progress::set(|loc, xid, skipped| {
        crate::logical::WalSndUpdateProgress(loc, xid, skipped);
        Ok(())
    });
}

// ---------------------------------------------------------------------------
// Small in-crate helpers that wrap a single owner-seam call, kept here so the
// module files read like the C (which calls the bare C name).
// ---------------------------------------------------------------------------

/// `max_wal_senders` GUC (the owned process-local value).
pub(crate) fn max_wal_senders_guc() -> i32 {
    core::proc_get(|p| p.max_wal_senders)
}

/// `LWLockReleaseAll()`.
pub(crate) fn lwlock_release_all() {
    backend_storage_lmgr_lwlock_seams::lwlock_release_all::call();
}

use backend_utils_activity_pgstat_io_seams as pgstat_io;

/// `pgstat_report_wait_end()` — clear this backend's reported wait event
/// (wait_event.c, owned by `backend-utils-activity-waitevent`).
pub(crate) fn pgstat_report_wait_end() {
    backend_utils_activity_waitevent_seams::pgstat_report_wait_end::call();
}

/// `pgaio_error_cleanup()`.
pub(crate) fn pgaio_error_cleanup() {
    backend_storage_aio_aio_seams::pgaio_error_cleanup::call();
}

/// `SetLatch(MyLatch)`.
pub(crate) fn set_latch_my_latch() {
    latch::set_latch_my_latch::call();
}

/// `whereToSendOutput = DestNone`.
pub(crate) fn set_where_to_send_output_none() {
    tcop::set_where_to_send_output_none::call();
}

/// `on_shmem_exit(WalSndKill, 0)` — register the slot-cleanup callback.
pub(crate) fn register_walsnd_kill_on_shmem_exit() {
    fn walsnd_kill_cb(
        _code: i32,
        _arg: types_tuple::Datum<'static>,
    ) -> types_error::PgResult<()> {
        crate::init::WalSndKill();
        Ok(())
    }
    ipc::on_shmem_exit::call(walsnd_kill_cb, types_tuple::Datum::ByVal(0))
        .expect("register WalSndKill on_shmem_exit");
}

// ---- loop / interrupt / config-reload wrappers ----

pub(crate) fn reset_latch_my_latch_loop() {
    latch::reset_latch_my_latch::call();
}

pub(crate) fn check_for_interrupts() {
    tcop::check_for_interrupts::call().expect("CHECK_FOR_INTERRUPTS");
}

pub(crate) fn config_reload_pending() -> bool {
    backend_postmaster_interrupt::ConfigReloadPending()
}

pub(crate) fn clear_config_reload_pending() {
    backend_postmaster_interrupt::SetConfigReloadPending(false);
}

pub(crate) fn process_config_file_sighup() {
    guc_file::process_config_file::call(types_guc::guc::GucContext::PGC_SIGHUP)
        .expect("ProcessConfigFile(PGC_SIGHUP)");
}

pub(crate) fn sync_rep_init_config() {
    syncrep::sync_rep_init_config::call();
}

/// `SyncRepRequested()` (syncrep.h) — `max_wal_senders > 0 &&
/// synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH`.
pub(crate) fn sync_rep_requested() -> bool {
    max_wal_senders_guc() > 0
        && xact::synchronous_commit::call() > types_core::xact::SYNCHRONOUS_COMMIT_LOCAL_FLUSH
}

// ---- libpq send-side wrappers ----

pub(crate) fn pq_is_send_pending() -> bool {
    pq::pq_is_send_pending::call()
}

pub(crate) fn pq_flush_if_writable() -> i32 {
    pq::pq_flush_if_writable::call()
}

pub(crate) fn pq_flush() {
    pq::pq_flush::call().expect("pq_flush");
}

pub(crate) fn pq_putmessage_noblock_output_message(msgtype: u8) {
    core::with_output_message(|b| pq::pq_putmessage_noblock::call(msgtype, b));
}

/// `pq_putmessage_noblock('c', NULL, 0)` — send CopyDone.
pub(crate) fn pq_putmessage_noblock_copydone() {
    pq::pq_putmessage_noblock::call(b'c', &[]);
}

pub(crate) fn modify_fe_be_wait_set_socket(events: u32) {
    pq::modify_fe_be_wait_set_socket::call(events);
}

pub(crate) fn wait_event_set_wait_fe_be(timeout: i64, wait_event_info: u32) -> (i32, u32) {
    pq::wait_event_set_wait_fe_be::call(timeout, wait_event_info)
}

// ---- misc owner-seam wrappers ----

pub(crate) fn proc_exit(code: i32) -> ! {
    ipc::proc_exit::call(code, miscinit::my_proc_pid::call())
}

pub(crate) fn standby_slots_have_caughtup(
    flushed_lsn: types_core::primitive::XLogRecPtr,
    elevel: i32,
) -> bool {
    slot::standby_slots_have_caughtup::call(flushed_lsn, types_error::ErrorLevel(elevel))
        .expect("StandbySlotsHaveCaughtup")
}

/// `ereport(COMMERROR, "terminating walsender process due to replication
/// timeout")`.  COMMERROR is LOG_SERVER_ONLY (< ERROR): emits server-side only,
/// then the caller proceeds to WalSndShutdown.
pub(crate) fn ereport_commerror_replication_timeout() {
    backend_utils_error::ereport(types_error::COMMERROR)
        .errmsg("terminating walsender process due to replication timeout")
        .finish(types_error::ErrorLocation::new(
            "walsender.c",
            0,
            "WalSndCheckTimeOut",
        ))
        .expect("ereport(COMMERROR) cannot fail below ERROR");
}

/// `WalSndSignals()` body — install the walsender signal handlers
/// (`pqsignal(...)` block), mirroring `WalReceiverMain`'s setup.
pub(crate) fn install_walsnd_signals() {
    use types_signal::SigHandler;
    let pqsignal = port_pqsignal_seams::pqsignal::call;

    fn config_reload(_sig: i32) {
        backend_postmaster_interrupt::SignalHandlerForConfigReload();
    }

    pqsignal(libc::SIGHUP, SigHandler::Handler(config_reload));
    pqsignal(libc::SIGINT, SigHandler::Handler(tcop::statement_cancel_handler::call)); // query cancel
    pqsignal(libc::SIGTERM, SigHandler::Handler(tcop::die::call)); // request shutdown
    // SIGQUIT handler was already set up by InitPostmasterChild.
    backend_utils_misc_timeout_seams::initialize_timeouts::call(); // establishes SIGALRM handler
    pqsignal(libc::SIGPIPE, SigHandler::Ignore);
    pqsignal(
        libc::SIGUSR1,
        SigHandler::Handler(procsignal::procsignal_sigusr1_handler::call),
    );
    pqsignal(
        libc::SIGUSR2,
        SigHandler::Handler(|_sig| crate::init::WalSndLastCycleHandler()),
    );

    // Reset some signals that are accepted by postmaster but not here.
    pqsignal(libc::SIGCHLD, SigHandler::Default);
}

#[cfg(test)]
mod tests;
