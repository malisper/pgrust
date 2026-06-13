//! Seam declarations for the `backend-utils-activity-pgstat-io` unit
//! (`utils/activity/pgstat_io.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::instrument::instr_time;

seam_core::seam!(
    /// `pgstat_prepare_io_time(track_io_timing)` — capture the I/O start time
    /// (zero `instr_time` when timing is disabled).
    pub fn pgstat_prepare_io_time() -> instr_time
);

seam_core::seam!(
    /// `pgstat_count_io_op_time(IOOBJECT_WAL, IOCONTEXT_NORMAL, IOOP_WRITE,
    /// start, 1, bytes_written)` — accumulate one WAL write into pg_stat_io.
    pub fn pgstat_count_io_op_time(start: instr_time, bytes_written: u32)
);
