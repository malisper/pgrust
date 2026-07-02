use types_core::ProcNumber;

seam_core::seam!(
    pub fn pg_semaphore_create(procno: ProcNumber)
);

seam_core::seam!(
    pub fn pg_semaphore_reset(procno: ProcNumber)
);

seam_core::seam!(
    pub fn pg_semaphore_lock(procno: ProcNumber)
);

seam_core::seam!(
    pub fn pg_semaphore_unlock(procno: ProcNumber)
);
