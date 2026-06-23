//! Outward seam declarations for the base-backup throttling sink
//! (`src/backend/backup/basebackup_throttle.c`, PostgreSQL 18.3).
//!
//! These are the cross-subsystem calls the throttle sink makes that have no
//! ported owner yet. Each is declared here (the consumer-owned seam
//! convention) and is installed from the owning subsystem's `init_seams()`
//! once that subsystem lands; until then a call panics loudly.
//!
//! The latch operations (`WaitLatch(MyLatch, …)` / `ResetLatch(MyLatch)`) are
//! NOT redeclared here: the latch unit (`storage/ipc/latch.c`) has already
//! landed and installs `wait_latch_my_latch` / `reset_latch_my_latch` in
//! `backend-storage-ipc-latch-seams`, which the throttle crate consumes
//! directly.

use ::types_core::primitive::TimestampTz;
use ::types_error::PgResult;

seam_core::seam!(
    /// `GetCurrentTimestamp()` (`utils/adt/timestamp.c`) — current
    /// transaction-stop timestamp in microseconds since the PostgreSQL epoch.
    /// Infallible in C; declared `PgResult` to match the workspace's
    /// timestamp-seam convention.
    pub fn get_current_timestamp() -> PgResult<TimestampTz>
);

seam_core::seam!(
    /// `CHECK_FOR_INTERRUPTS()` (miscadmin.h) — service any pending interrupt
    /// (query cancel, termination, recovery conflict). `Err` carries the
    /// `ProcessInterrupts` `ereport(ERROR/FATAL)`.
    pub fn check_for_interrupts() -> PgResult<()>
);
