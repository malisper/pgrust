use types_core::ProcNumber;

// `lwWaiting`/`lwWaitMode` travel as the raw uint8 PGPROC stores.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct proclist_node {
    pub next: ProcNumber,
    pub prev: ProcNumber,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct proclist_head {
    pub head: ProcNumber,
    pub tail: ProcNumber,
}

seam_core::seam!(
    pub fn proc_lw_waiting(procno: ProcNumber) -> u8
);

seam_core::seam!(
    pub fn set_proc_lw_waiting(procno: ProcNumber, state: u8)
);

seam_core::seam!(
    pub fn proc_lw_wait_mode(procno: ProcNumber) -> u8
);

seam_core::seam!(
    pub fn set_proc_lw_wait_mode(procno: ProcNumber, mode: u8)
);

seam_core::seam!(
    pub fn proc_lw_wait_link(procno: ProcNumber) -> proclist_node
);

seam_core::seam!(
    pub fn set_proc_lw_wait_link(procno: ProcNumber, node: proclist_node)
);

seam_core::seam!(
    pub fn pg_semaphore_lock(procno: ProcNumber)
);

seam_core::seam!(
    pub fn pg_semaphore_unlock(procno: ProcNumber)
);

seam_core::seam!(
    // &ProcGlobal->allProcs[procno].procLatch — shmem is process-lifetime.
    // Reachable from SetLatch in signal handlers: impl must be allocation-free.
    pub fn proc_latch(procno: ProcNumber) -> &'static types_storage::latch::Latch
);
