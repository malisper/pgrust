seam_core::seam!(
    pub fn pgstat_progress_end_command()
);

seam_core::seam!(
    pub fn pgstat_progress_update_param(index: usize, val: i64)
);

seam_core::seam!(
    pub fn pgstat_progress_update_multi_param<'a>(indices: &'a [usize], vals: &'a [i64])
);

// Lock holder wait counts (commands/progress.h): params 3-5 are reserved for
// the "waitfor" metrics in both the CREATE INDEX and CLUSTER progress views.
// They live here so low-level waiters (lmgr) can report without depending on
// the backend_progress crate itself.
pub const PROGRESS_WAITFOR_TOTAL: usize = 3;
pub const PROGRESS_WAITFOR_DONE: usize = 4;
pub const PROGRESS_WAITFOR_CURRENT_PID: usize = 5;

// Block counts of the heap scan (commands/progress.h), shared by the CREATE
// INDEX and CLUSTER views; the table-AM build scan (execindexing) reports
// them through the seam for the same reason.
pub const PROGRESS_SCAN_BLOCKS_TOTAL: usize = 15;
pub const PROGRESS_SCAN_BLOCKS_DONE: usize = 16;
