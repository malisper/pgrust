//! Seam declarations for the `backend-storage-lmgr-proc` unit
//! (`storage/lmgr/proc.c`): the PGPROC array fields the LWLock wait-list
//! machinery reads and writes (`GetPGProcByNumber(procno)->lwWaiting /
//! lwWaitMode / lwWaitLink`) and the per-process wait semaphore
//! (`PGSemaphoreLock` / `PGSemaphoreUnlock` on `GetPGProcByNumber(procno)->sem`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::ProcNumber;
use types_storage::{proclist_node, LWLockMode, LWLockWaitState};

seam_core::seam!(
    /// Read `GetPGProcByNumber(procno)->lwWaiting`.
    pub fn proc_lw_waiting(procno: ProcNumber) -> LWLockWaitState
);

seam_core::seam!(
    /// Set `GetPGProcByNumber(procno)->lwWaiting`.
    pub fn set_proc_lw_waiting(procno: ProcNumber, state: LWLockWaitState)
);

seam_core::seam!(
    /// Read `GetPGProcByNumber(procno)->lwWaitMode`.
    pub fn proc_lw_wait_mode(procno: ProcNumber) -> LWLockMode
);

seam_core::seam!(
    /// Set `GetPGProcByNumber(procno)->lwWaitMode`.
    pub fn set_proc_lw_wait_mode(procno: ProcNumber, mode: LWLockMode)
);

seam_core::seam!(
    /// Read `GetPGProcByNumber(procno)->lwWaitLink` (the C
    /// `proclist_node_get(procno, offsetof(PGPROC, lwWaitLink))`).
    pub fn proc_lw_wait_link(procno: ProcNumber) -> proclist_node
);

seam_core::seam!(
    /// Write `GetPGProcByNumber(procno)->lwWaitLink`.
    pub fn set_proc_lw_wait_link(procno: ProcNumber, node: proclist_node)
);

seam_core::seam!(
    /// `PGSemaphoreLock(GetPGProcByNumber(procno)->sem)` — block the current
    /// backend on its wait semaphore until signaled.
    pub fn pg_semaphore_lock(procno: ProcNumber)
);

seam_core::seam!(
    /// `PGSemaphoreUnlock(GetPGProcByNumber(procno)->sem)` — signal a
    /// backend's wait semaphore.
    pub fn pg_semaphore_unlock(procno: ProcNumber)
);

seam_core::seam!(
    /// `&GetPGProcByNumber(procno)->procLatch` — the process latch embedded
    /// in a backend's PGPROC entry, as a handle usable with the latch seams
    /// (`set_latch` to wake that backend). Pure array lookup; infallible.
    pub fn proc_latch(procno: ProcNumber) -> types_storage::latch::LatchHandle
);
