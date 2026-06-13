//! `backend-storage-ipc` — POSTGRES inter-process communication
//! initialization (`src/backend/storage/ipc/ipci.c`).
//!
//! # Scaffold stage
//!
//! This crate is the `ipci.c` port-in-progress. Every public function below
//! carries its real, C-faithful signature with a `todo!()` body; the
//! shared-memory subsystem boundaries it routes across are wired to the
//! owners' per-owner `*-seams` crates. The crate compiles so dependents can
//! build against the stable surface while the bodies are filled in.
//!
//! ## Families (one module per concern)
//!
//! | Module                          | Concern                                                            |
//! |---------------------------------|--------------------------------------------------------------------|
//! | [`ipci_core`]                   | `ipci.c` itself: `RequestAddinShmemSpace`, `CalculateShmemSize`, `CreateSharedMemoryAndSemaphores`, `CreateOrAttachShmemStructs`, `AttachSharedMemoryStructs`, `InitializeShmemGUCs` |
//! | [`ipci_seams_storage_access`]   | per-owner `*ShmemSize`/`*ShmemInit` seam routing for the storage-access subsystems (bufmgr/lock/predicate/procarray/sinval/pmsignal/procsignal/aio/syncscan/nbtree) |
//! | [`ipci_seams_xlog_clog`]        | per-owner `*ShmemSize`/`*ShmemInit` seam routing for the WAL/CLOG subsystems (varsup/xlog/xlogprefetcher/xlogrecovery/clog/commit_ts/subtrans/multixact/twophase) |
//! | [`ipci_seams_bgworker_repl_stats`] | per-owner `*ShmemSize`/`*ShmemInit` seam routing for the bgworker/replication/stats subsystems (checkpointer/autovacuum/bgworker/walsummarizer/pgarch/walsender/walreceiver/slot/origin/launcher/slotsync/async/pgstat/waitevent/injection_point) |
//!
//! ## ipc.c is NOT here
//!
//! `storage/ipc/ipc.c` (the `proc_exit`/`shmem_exit` callback machinery and
//! the `backend-storage-ipc-seams` slots) is already ported in
//! `backend-storage-ipc-dsm-core` (its cycle partner). This unit reaches the
//! `on_shmem_exit`/`proc_exit` family through `backend-storage-ipc-seams`
//! exactly like any other consumer; it does not re-port them.

#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

pub mod ipci_core;
pub mod ipci_seams_bgworker_repl_stats;
pub mod ipci_seams_storage_access;
pub mod ipci_seams_xlog_clog;

/// Install every seam this unit owns.
///
/// ipci.c installs no inward seams of its own — nothing in PostgreSQL calls
/// back into `CreateSharedMemoryAndSemaphores`/`CalculateShmemSize` across a
/// dependency cycle; they are leaf entry points invoked from the postmaster
/// bootstrap. (The `proc_exit`/`shmem_exit` family that *would* be seamed
/// lives in `ipc.c`, owned by `backend-storage-ipc-dsm-core`.) This stays an
/// empty placeholder for the uniform `seams-init` shape.
pub fn init_seams() {}
