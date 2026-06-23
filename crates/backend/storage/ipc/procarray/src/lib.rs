//! `backend-storage-ipc-procarray` — port of `src/backend/storage/ipc/procarray.c`:
//! the shared `ProcArray` (the set of running backends + their advertised
//! xid/xmin/subxids), the running-transactions snapshot, the xmin/removable
//! horizon machinery (`GlobalVisState`), the hot-standby `KnownAssignedXids`
//! ring, and the per-xid visibility / backend-lookup / counting helpers.
//!
//! procarray.c is large, so it is split into the family modules a NEEDS_DECOMP
//! requires; each mirrors a coherent slice of the C file and panics
//! (`panic!("decomp: <fn> not yet filled")`) until the fill stage lands its
//! real logic.
//!
//! - [`shmem_model`] — F0 KEYSTONE: the in-shmem `ProcArrayStruct`, the dense
//!   per-slot mirror, the real `GlobalVisState` struct, the file-static
//!   process-locals, `ProcArrayShmem{Size,Init}`, `GetMaxSnapshot{Xid,Subxid}Count`,
//!   and the `FullTransactionId`/`TransactionId` arithmetic helpers.
//! - [`membership`] — F1: slot add/remove + end-of-xact membership +
//!   group-clear.
//! - [`snapshot`] — F2: `GetSnapshotData`, running-xacts, replication-slot
//!   xmin, logical-decoding flag/horizon.
//! - [`horizons`] — F3: `ComputeXidHorizons` + the `GlobalVisState` family;
//!   owns the NEW `global_vis_test_*` / `get_oldest_non_removable_transaction_id`
//!   inward seams.
//! - [`visibility_lookup`] — F4: per-xid visibility tests + backend/vxid lookup
//!   + counting.
//! - [`knownassignedxids`] — F5: the hot-standby `KnownAssignedXids` ring +
//!   recovery xid bookkeeping.
//!
//! OUTWARD seams this unit reaches (all through the owner's `-seams` crate,
//! panicking until filled): `backend-storage-ipc-shmem-seams`
//! (`ShmemInitStruct`/`add_size`/`mul_size`), `backend-storage-lmgr-lwlock`
//! (`ProcArrayLock`/`XidGenLock`), `backend-storage-lmgr-proc-seams` (PGPROC /
//! ProcGlobal field access, `SendProcSignal`/`ProcSendSignal`,
//! `MyProc`/`MyProcNumber`/`MyPgXactOff`), the transam / subtrans / clog /
//! twophase / xlog seams, snapmgr handle resolution, and the latch/waitevent
//! seams for delaychkpt waits.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

pub mod horizons;
pub mod knownassignedxids;
pub mod membership;
pub mod shmem_model;
pub mod snapshot;
pub mod visibility_lookup;

/// Install every inward seam this unit owns
/// (`backend-storage-ipc-procarray-seams`), wiring each family's
/// implementations. Called once from `seams-init::init_all`.
pub fn init_seams() {
    shmem_model::init_seams();
    membership::init_seams();
    snapshot::init_seams();
    horizons::init_seams();
    visibility_lookup::init_seams();
    knownassignedxids::init_seams();
}
