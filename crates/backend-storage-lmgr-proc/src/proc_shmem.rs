//! Shared-memory sizing and one-time initialization (`storage/lmgr/proc.c`).
//!
//! `InitProcGlobal` runs once in the postmaster: it carves the `PGPROC` array
//! and the dense `ProcGlobal` mirror arrays (`xids`/`subxidStates`/
//! `statusFlags`) out of shared memory, initializes each `PGPROC`'s embedded
//! latch / semaphore / fast-path arrays, and threads every entry onto one of
//! the four freelists (`freeProcs` / `autovacFreeProcs` / `bgworkerFreeProcs`
//! / `walsenderFreeProcs`) by backend class.
//!
//! RECLAIMED here (real algorithm, not a seam): the freelist `dlist` push over
//! the real `PGPROC` array — the by-class partitioning that `InitProcGlobal`
//! performs and that `InitProcess` later pops from.
//!
//! SCAFFOLD STAGE: bodies are `todo!()`.

use mcx::Mcx;
use types_core::Size;
use types_error::PgResult;

/// `PGProcShmemSize(void)` — bytes for the `PGPROC` array (regular + special
/// worker + aux + prepared-xact dummies) plus the dense mirror arrays.
pub fn PGProcShmemSize() -> Size {
    todo!("proc.c:PGProcShmemSize")
}

/// `FastPathLockShmemSize(void)` — bytes for the per-backend fast-path lock
/// bit/relid arrays referenced from each `PGPROC`.
pub fn FastPathLockShmemSize() -> Size {
    todo!("proc.c:FastPathLockShmemSize")
}

/// `ProcGlobalShmemSize(void)` — total shared memory for the proc subsystem
/// (`PROC_HDR` + [`PGProcShmemSize`] + [`FastPathLockShmemSize`] + semaphores).
pub fn ProcGlobalShmemSize() -> Size {
    todo!("proc.c:ProcGlobalShmemSize")
}

/// `ProcGlobalSemas(void)` — number of PGSemaphores the proc subsystem needs.
pub fn ProcGlobalSemas() -> i32 {
    todo!("proc.c:ProcGlobalSemas")
}

/// `InitProcGlobal(void)` — postmaster-time setup: build the `PGPROC` array,
/// the dense `ProcGlobal` mirror arrays, the embedded latches/semaphores/
/// fast-path arrays, and the four by-class freelists.
pub fn InitProcGlobal(_mcx: Mcx<'_>) -> PgResult<()> {
    todo!("proc.c:InitProcGlobal")
}
