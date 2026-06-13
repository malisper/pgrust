//! Seam-signature vocabulary for the logical-replication parallel-apply
//! coordinator (`replication/logical/applyparallelworker.c` and the
//! `worker_internal.h` shared header it threads).
//!
//! Each type is consumed by the `backend-replication-logical-applyparallelworker`
//! crate and the per-owner seam crates whose signatures carry it. Layouts and
//! discriminant order are verified against the C headers (`storage/shm_mq.h`,
//! `replication/worker_internal.h`).
#![no_std]
#![allow(non_camel_case_types)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use types_core::Oid;

/// `FileSet` (`storage/fileset.h`) — a set of named temporary files. The
/// parallel-apply shared header keeps a copy (`ParallelApplyWorkerShared.fileset`)
/// so the leader can hand the serialized-changes fileset to the parallel apply
/// worker and re-use it for the next streaming transaction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct FileSet {
    /// `pid_t creator_pid` — PID of the creating process.
    pub creator_pid: i32,
    /// `uint32 number` — per-PID identifier.
    pub number: u32,
    /// `int ntablespaces` — number of tablespaces to use.
    pub ntablespaces: i32,
    /// `Oid tablespaces[8]` — OIDs of tablespaces to use.
    pub tablespaces: [Oid; 8],
}

/// `shm_mq_result` (`storage/shm_mq.h`) — outcome of a non-blocking send/receive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum ShmMqResult {
    SHM_MQ_SUCCESS,
    SHM_MQ_WOULD_BLOCK,
    SHM_MQ_DETACHED,
}

/// A received message plus its `shm_mq_result`, returned by `shm_mq_receive_*`.
pub struct ShmMqReceived {
    pub result: ShmMqResult,
    /// The message bytes (valid only when `result == SHM_MQ_SUCCESS`).
    pub data: Vec<u8>,
}

/// `ErrorData` subset carried out of `pq_parse_errornotice` — only the
/// `context` line `ProcessParallelApplyMessage` touches.
pub struct ParsedErrorNotice {
    /// `edata.context` (may be absent).
    pub context: Option<String>,
}

/// The DSM-resident handles `pa_setup_dsm` writes back into the winfo on
/// success. The `ParallelApplyWorkerShared` header itself is owned and created
/// in-crate (the coordinator owns `MyParallelShared`); only the segment and the
/// two `shm_mq` queues are owned by the DSM/shm_mq machinery, so only their
/// handles cross the seam.
pub struct DsmSetupResult {
    /// `winfo->dsm_seg`.
    pub dsm_seg: u64,
    /// `winfo->mq_handle`.
    pub mq_handle: u64,
    /// `winfo->error_mq_handle`.
    pub error_mq_handle: u64,
}

/// `ParallelTransState` (`worker_internal.h`) — the transaction-progress state
/// the parallel apply worker advances and the leader waits on. The discriminant
/// *order* matters: `pa_wait_for_xact_state` compares with `>=`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(C)]
pub enum ParallelTransState {
    PARALLEL_TRANS_UNKNOWN = 0,
    PARALLEL_TRANS_STARTED = 1,
    PARALLEL_TRANS_FINISHED = 2,
}

/// `PartialFileSetState` (`worker_internal.h`) — state of the leader→worker
/// serialize-changes fileset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum PartialFileSetState {
    FS_EMPTY = 0,
    FS_SERIALIZE_IN_PROGRESS = 1,
    FS_SERIALIZE_DONE = 2,
    FS_READY = 3,
}
