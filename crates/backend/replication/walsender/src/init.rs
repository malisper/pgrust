//! Walsender process init / teardown / shmem / signal handling.
//!
//! Port of the init/teardown/shmem/signal section of `walsender.c`:
//!  * `InitWalSender`, `WalSndErrorCleanup`, `WalSndShutdown`
//!  * `InitWalSenderSlot`, `WalSndKill`
//!  * `WalSndSignals`, `WalSndLastCycleHandler`
//!  * `WalSndShmemSize`, `WalSndShmemInit`
//!  * `WalSndSetState`, `WalSndGetStateString`

#![allow(non_snake_case)]

use crate::core::{
    proc_get, set_wal_snd_ctl, walsnds_slot_mut, wal_snd_ctl_mut, with_proc, InvalidOid,
    LagTracker, Size, WalSndCtlData, WalSndSlot, WalSndState, NUM_SYNC_REP_WAIT_MODE,
};
use crate::{condvar, ipc, miscinit, procarray, resowner, shmem, tcop, xlog};

/// `void InitWalSender(void)` — initialize walsender before the main command loop.
pub fn InitWalSender() {
    let in_recovery = xlog::recovery_in_progress::call();
    with_proc(|p| p.am_cascading_walsender = in_recovery);

    // Create a per-walsender data structure in shared memory.
    InitWalSenderSlot();

    // Need a resource owner for e.g. basebackups.
    resowner::create_aux_process_resource_owner::call().expect("CreateAuxProcessResourceOwner");

    // Let postmaster know that we're a WAL sender, so it lets us outlive
    // bgwriter and kills us last.  There's no going back; we mustn't write any
    // WAL after this.
    postmaster_seams::mark_postmaster_child_wal_sender::call();
    postmaster_seams::send_postmaster_signal_advance_state_machine::call();

    // If the client didn't specify a database, advertise that our xmin should
    // affect vacuum horizons in all databases (for physical hot-standby
    // feedback).
    if miscinit::my_database_id::call() == InvalidOid {
        debug_assert_eq!(
            procarray::my_proc_xmin::call(),
            types_core::InvalidTransactionId
        );
        procarray::set_proc_affects_all_horizons::call();
    }

    // Initialize empty timestamp buffer for lag tracking.
    with_proc(|p| p.lag_tracker = Some(LagTracker::zeroed()));
}

/// `void WalSndErrorCleanup(void)` — clean up after an error during WAL sending.
pub fn WalSndErrorCleanup() {
    // LWLockReleaseAll() / ConditionVariableCancelSleep() / pgstat_report_wait_end()
    // / pgaio_error_cleanup() are reached through the storage/pgstat owners.
    crate::lwlock_release_all();
    condvar::condition_variable_cancel_sleep::call();
    crate::pgstat_report_wait_end();
    crate::pgaio_error_cleanup();

    crate::start_replication::xlogreader_close_if_open();

    if crate::slot::my_replication_slot_is_set::call() {
        let _ = crate::slot::replication_slot_release::call();
    }

    let _ = crate::slot::replication_slot_cleanup::call(false);

    with_proc(|p| p.replication_active = 0);

    // If a replication command set up a resource owner without a transaction,
    // clean that up now.
    if !crate::xact::is_transaction_or_transaction_block::call() {
        resowner::release_aux_process_resources::call(false)
            .expect("ReleaseAuxProcessResources");
    }

    if proc_get(|p| p.got_STOPPING != 0 || p.got_SIGUSR2 != 0) {
        ipc::proc_exit::call(0, miscinit::my_proc_pid::call());
    }

    // Revert back to startup state.
    WalSndSetState(WalSndState::WALSNDSTATE_STARTUP);
}

/// `pg_noreturn static void WalSndShutdown(void)` — close the connection and exit.
pub fn WalSndShutdown() -> ! {
    // Reset whereToSendOutput so ereport doesn't try to message the standby.
    if tcop::where_to_send_output::call() == types_dest::dest::CommandDest::Remote {
        crate::set_where_to_send_output_none();
    }
    ipc::proc_exit::call(0, miscinit::my_proc_pid::call())
}

/// `static void InitWalSenderSlot(void)`.
pub fn InitWalSenderSlot() {
    let my_pid = miscinit::my_proc_pid::call();
    let db_invalid = miscinit::my_database_id::call() == InvalidOid;
    crate::shmem_array::reserve_slot(my_pid, db_invalid);

    // Arrange to clean up at walsender exit.
    crate::register_walsnd_kill_on_shmem_exit();
}

/// `static void WalSndKill(int code, Datum arg)`.
pub fn WalSndKill() {
    crate::shmem_array::release_my_slot();
}

/// `void WalSndSignals(void)` — set up signal handlers.
pub fn WalSndSignals() {
    crate::install_walsnd_signals();
}

/// `static void WalSndLastCycleHandler(SIGNAL_ARGS)` — SIGUSR2 handler.
pub fn WalSndLastCycleHandler() {
    with_proc(|p| p.got_SIGUSR2 = 1);
    crate::set_latch_my_latch();
}

/// `Size WalSndShmemSize(void)`.
pub fn WalSndShmemSize() -> Size {
    // size = offsetof(WalSndCtlData, walsnds);
    // size = add_size(size, mul_size(max_wal_senders, sizeof(WalSnd)));
    let header = core::mem::size_of::<WalSndCtlData>();
    let max = crate::max_wal_senders_guc() as usize;
    let per = core::mem::size_of::<WalSndSlot>();
    let arr = shmem::mul_size::call(max, per).expect("WalSndShmemSize: mul_size overflow");
    shmem::add_size::call(header, arr).expect("WalSndShmemSize: add_size overflow")
}

/// `void WalSndShmemInit(void)`.
pub fn WalSndShmemInit() {
    let size = WalSndShmemSize();
    let (ptr, found) = shmem::shmem_init_struct::call("Wal Sender Ctl", size)
        .expect("WalSndShmemInit: ShmemInitStruct");
    let ctl_ptr = ptr as *mut WalSndCtlData;

    if !found {
        // First time through, so initialize.
        unsafe {
            core::ptr::write_bytes(ptr, 0, size);
            // Initialize the fixed header in place (CVs / queues / status).
            let header = WalSndCtlData {
                SyncRepQueue: core::array::from_fn(|_| {
                    types_storage::storage::proclist_head::default()
                }),
                lsn: [0; NUM_SYNC_REP_WAIT_MODE],
                sync_standbys_status: 0,
                wal_flush_cv: condvar::ConditionVariable::new(),
                wal_replay_cv: condvar::ConditionVariable::new(),
                wal_confirm_rcv_cv: condvar::ConditionVariable::new(),
            };
            core::ptr::write(ctl_ptr, header);
        }

        set_wal_snd_ctl(ctl_ptr);

        // The SyncRepQueue heads, the three CVs, and the slot mutexes were
        // written in their initialized state above; dlist_init each queue head
        // (C `dlist_init`, here `proclist_init` to the empty head/tail),
        // SpinLockInit each slot mutex and ConditionVariableInit the three CVs.
        let ctl = wal_snd_ctl_mut();
        for q in ctl.SyncRepQueue.iter_mut() {
            *q = types_storage::storage::proclist_head::default();
        }
        let max = crate::max_wal_senders_guc();
        let mut i: i32 = 0;
        while i < max {
            let slot = walsnds_slot_mut(i);
            s_lock::s_init_lock(&slot.mutex);
            i += 1;
        }
        condvar::condition_variable_init::call(&mut ctl.wal_flush_cv);
        condvar::condition_variable_init::call(&mut ctl.wal_replay_cv);
        condvar::condition_variable_init::call(&mut ctl.wal_confirm_rcv_cv);
    } else {
        set_wal_snd_ctl(ctl_ptr);
    }
}

/// `void WalSndSetState(WalSndState state)`.
pub fn WalSndSetState(state: WalSndState) {
    debug_assert!(proc_get(|p| p.am_walsender));

    if crate::shmem_array::my_state() == state {
        return;
    }
    crate::shmem_array::my_set_state(state);
}

/// `static const char *WalSndGetStateString(WalSndState state)`.
pub fn WalSndGetStateString(state: WalSndState) -> &'static str {
    match state {
        WalSndState::WALSNDSTATE_STARTUP => "startup",
        WalSndState::WALSNDSTATE_BACKUP => "backup",
        WalSndState::WALSNDSTATE_CATCHUP => "catchup",
        WalSndState::WALSNDSTATE_STREAMING => "streaming",
        WalSndState::WALSNDSTATE_STOPPING => "stopping",
    }
}
