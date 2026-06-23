//! Seam declarations for the `backend-utils-activity-pgstat-io` unit
//! (`utils/activity/pgstat_io.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::instrument::instr_time;
use types_pgstat::activity_pgstat::{IOContext, IOObject, IOOp};

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

seam_core::seam!(
    /// `pgstat_count_io_op_time(IOOBJECT_WAL, IOCONTEXT_NORMAL, IOOP_READ,
    /// start, 1, readbytes)` (xlogreader.c `WALRead`) — accumulate one WAL read
    /// into pg_stat_io. Distinct from the WAL-write seam above: the recovery
    /// `XLogPageRead` reads WAL pages and must record them as IOOP_READ (the
    /// startup process's pg_stat_io `reads`), not IOOP_WRITE.
    pub fn pgstat_count_io_op_time_wal_read(start: instr_time, readbytes: u32)
);

seam_core::seam!(
    /// `pgstat_flush_io(nowait)` (pgstat_io.c) — flush pending pg_stat_io
    /// counts to shared memory. Returns whether some stats were left unflushed
    /// (the walsender caller discards it).
    pub fn pgstat_flush_io(nowait: bool) -> bool
);

seam_core::seam!(
    /// `pgstat_flush_backend(nowait, PGSTAT_BACKEND_FLUSH_IO)` (pgstat_backend.c)
    /// — flush this backend's per-backend I/O stats. Returns whether some were
    /// left unflushed.
    pub fn pgstat_flush_backend_io(nowait: bool) -> bool
);

seam_core::seam!(
    /// `pgstat_count_backend_io_op(io_object, io_context, io_op, cnt, bytes)`
    /// (`pgstat_backend.c`) — accumulate one I/O into this backend's per-backend
    /// pending stats (PGSTAT_KIND_BACKEND). Owned by `pgstat_backend.c`, which is
    /// not yet ported, so it remains seam-and-panic.
    pub fn pgstat_count_backend_io_op(
        io_object: IOObject,
        io_context: IOContext,
        io_op: IOOp,
        cnt: u32,
        bytes: u64,
    )
);

seam_core::seam!(
    /// `pgstat_count_backend_io_op_time(io_object, io_context, io_op, io_time)`
    /// (`pgstat_backend.c`) — accumulate one I/O's elapsed time into this
    /// backend's per-backend pending stats. Owned by `pgstat_backend.c`
    /// (unported), so it remains seam-and-panic.
    pub fn pgstat_count_backend_io_op_time(
        io_object: IOObject,
        io_context: IOContext,
        io_op: IOOp,
        io_time: instr_time,
    )
);
