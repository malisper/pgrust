//! Cross-process wakeup, stop coordination, and file-reload signaling.
//!
//! 1:1 port of:
//!  * `WalSndWakeup`
//!  * `WalSndInitStopping`, `WalSndWaitStopping`, `HandleWalSndInitStopping`
//!  * `WalSndRqstFileReload`
//!
//! The shared-memory `WalSnd` array (and its CVs) is owned by [`crate::core`];
//! per-slot reads/writes are real functions in [`crate::shmem_array`].

#![allow(non_snake_case)]

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use crate::core::{proc_get, with_proc, wal_snd_ctl, WalSndState};
use crate::{condvar, miscinit, procsignal};

/// `void WalSndWakeup(bool physical, bool logical)`.
pub fn WalSndWakeup(physical: bool, logical: bool) {
    // Wake all walsenders waiting on WAL being flushed or replayed respectively.
    if physical {
        condvar::condition_variable_broadcast::call(&wal_snd_ctl().wal_flush_cv);
    }
    if logical {
        condvar::condition_variable_broadcast::call(&wal_snd_ctl().wal_replay_cv);
    }
}

/// `if (AllowCascadeReplication()) WalSndWakeup(true, false)` — wake cascading
/// walsenders after the walreceiver flushes new WAL.
///
/// `AllowCascadeReplication()` (`replication/walsender.h`) =
/// `EnableHotStandby && max_wal_senders > 0`.
pub fn WalSndWakeupIfCascading() {
    let allow = crate::xlog::enable_hot_standby::call() && crate::max_wal_senders_guc() > 0;
    if allow {
        WalSndWakeup(true, false);
    }
}

/// `void WalSndInitStopping(void)`.
pub fn WalSndInitStopping() {
    let max = proc_get(|p| p.max_wal_senders);
    let mut i: i32 = 0;
    while i < max {
        let pid = crate::shmem_array::slot_pid(i);
        if pid == 0 {
            i += 1;
            continue;
        }
        let _ = procsignal::send_proc_signal::call(
            pid,
            types_storage::ProcSignalReason::PROCSIG_WALSND_INIT_STOPPING,
            types_core::INVALID_PROC_NUMBER,
        );
        i += 1;
    }
}

/// `void WalSndWaitStopping(void)`.
pub fn WalSndWaitStopping() {
    let max = proc_get(|p| p.max_wal_senders);
    loop {
        let mut all_stopped = true;

        let mut i: i32 = 0;
        while i < max {
            let snap = crate::shmem_array::slot_snapshot(i);
            if snap.pid == 0 {
                i += 1;
                continue;
            }
            if snap.state != WalSndState::WALSNDSTATE_STOPPING {
                all_stopped = false;
                break;
            }
            i += 1;
        }

        // Safe to leave once confirmation is done for all WAL senders.
        if all_stopped {
            return;
        }

        // wait for 10 msec.
        pgsleep::pg_usleep(10000);
    }
}

/// `void HandleWalSndInitStopping(void)` — PROCSIG_WALSND_INIT_STOPPING body.
pub fn HandleWalSndInitStopping() {
    debug_assert!(proc_get(|p| p.am_walsender));

    // If replication has not yet started, die like with SIGTERM.  If active,
    // only set a flag and let the main loop drain WAL, wait for replication,
    // then exit gracefully.
    if proc_get(|p| p.replication_active) == 0 {
        // kill(MyProcPid, SIGTERM)
        let pid = miscinit::my_proc_pid::call();
        // SAFETY: kill(2) with our own pid and SIGTERM; touches no memory.
        unsafe {
            let _ = libc::kill(pid, libc::SIGTERM);
        }
    } else {
        with_proc(|p| p.got_STOPPING = 1);
    }
}

/// `void WalSndRqstFileReload(void)`.
pub fn WalSndRqstFileReload() {
    crate::shmem_array::set_all_needreload();
}
