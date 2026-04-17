use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex, RwLock};

use crate::backend::access::transam::xact::TransactionManager;
use crate::backend::access::transam::{ControlFileState, ControlFileStore};
use crate::backend::access::transam::xlog::{INVALID_LSN, Lsn, WalWriter};
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::buffer::{BufferPool, Error as BufferError};
use crate::backend::utils::misc::checkpoint::{
    CheckpointCompletionKind, CheckpointConfig, CheckpointStatsSnapshot,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointRequestFlags {
    pub wait: bool,
    pub immediate: bool,
    pub force: bool,
    pub shutdown: bool,
}

impl CheckpointRequestFlags {
    pub const fn sql() -> Self {
        Self {
            wait: true,
            immediate: true,
            force: true,
            shutdown: false,
        }
    }

    pub const fn shutdown() -> Self {
        Self {
            wait: true,
            immediate: true,
            force: true,
            shutdown: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointRecord {
    pub redo_lsn: Lsn,
}

#[derive(Default)]
struct CheckpointBarrierState {
    active_commits: usize,
    checkpoint_active: bool,
}

pub struct CheckpointCommitBarrier {
    state: Mutex<CheckpointBarrierState>,
    cv: Condvar,
}

impl CheckpointCommitBarrier {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(CheckpointBarrierState::default()),
            cv: Condvar::new(),
        }
    }

    pub fn enter(self: &Arc<Self>) -> CheckpointCommitGuard {
        let mut state = self.state.lock();
        while state.checkpoint_active {
            self.cv.wait(&mut state);
        }
        state.active_commits = state.active_commits.saturating_add(1);
        drop(state);
        CheckpointCommitGuard {
            barrier: Arc::clone(self),
            released: false,
        }
    }

    pub fn begin_checkpoint(self: &Arc<Self>) -> CheckpointBarrierGuard {
        let mut state = self.state.lock();
        while state.checkpoint_active {
            self.cv.wait(&mut state);
        }
        state.checkpoint_active = true;
        while state.active_commits > 0 {
            self.cv.wait(&mut state);
        }
        drop(state);
        CheckpointBarrierGuard {
            barrier: Arc::clone(self),
            released: false,
        }
    }

    fn release_commit(&self) {
        let mut state = self.state.lock();
        state.active_commits = state.active_commits.saturating_sub(1);
        if state.active_commits == 0 {
            self.cv.notify_all();
        }
    }

    fn finish_checkpoint(&self) {
        let mut state = self.state.lock();
        state.checkpoint_active = false;
        self.cv.notify_all();
    }
}

pub struct CheckpointCommitGuard {
    barrier: Arc<CheckpointCommitBarrier>,
    released: bool,
}

impl Drop for CheckpointCommitGuard {
    fn drop(&mut self) {
        if !self.released {
            self.released = true;
            self.barrier.release_commit();
        }
    }
}

pub struct CheckpointBarrierGuard {
    barrier: Arc<CheckpointCommitBarrier>,
    released: bool,
}

impl Drop for CheckpointBarrierGuard {
    fn drop(&mut self) {
        if !self.released {
            self.released = true;
            self.barrier.finish_checkpoint();
        }
    }
}

#[derive(Clone, Copy)]
enum CheckpointTrigger {
    Timed,
    Requested,
    Shutdown,
}

struct CheckpointerState {
    requested_seq: u64,
    completed_seq: u64,
    failed_seq: u64,
    last_error: Option<String>,
    shutdown_requested: bool,
    worker_exited: bool,
    last_checkpoint_lsn: Lsn,
}

struct CheckpointExecution {
    end_lsn: Lsn,
}

pub struct Checkpointer {
    pool: Arc<BufferPool<SmgrStorageBackend>>,
    wal: Option<Arc<WalWriter>>,
    txns: Arc<RwLock<TransactionManager>>,
    control_file: Option<Arc<ControlFileStore>>,
    config: Arc<CheckpointConfig>,
    stats: Arc<RwLock<CheckpointStatsSnapshot>>,
    commit_barrier: Arc<CheckpointCommitBarrier>,
    state: Mutex<CheckpointerState>,
    cv: Condvar,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl Checkpointer {
    pub fn start(
        pool: Arc<BufferPool<SmgrStorageBackend>>,
        wal: Option<Arc<WalWriter>>,
        txns: Arc<RwLock<TransactionManager>>,
        control_file: Option<Arc<ControlFileStore>>,
        config: Arc<CheckpointConfig>,
        stats: Arc<RwLock<CheckpointStatsSnapshot>>,
        commit_barrier: Arc<CheckpointCommitBarrier>,
    ) -> Arc<Self> {
        let initial_lsn = wal.as_ref().map(|wal| wal.insert_lsn()).unwrap_or(INVALID_LSN);
        let checkpointer = Arc::new(Self {
            pool,
            wal,
            txns,
            control_file,
            config,
            stats,
            commit_barrier,
            state: Mutex::new(CheckpointerState {
                requested_seq: 0,
                completed_seq: 0,
                failed_seq: 0,
                last_error: None,
                shutdown_requested: false,
                worker_exited: false,
                last_checkpoint_lsn: initial_lsn,
            }),
            cv: Condvar::new(),
            handle: Mutex::new(None),
        });
        let worker = Arc::clone(&checkpointer);
        let handle = thread::Builder::new()
            .name("checkpointer".into())
            .spawn(move || worker.worker_main())
            .expect("failed to spawn checkpointer thread");
        *checkpointer.handle.lock() = Some(handle);
        checkpointer
    }

    pub fn request(&self, flags: CheckpointRequestFlags) -> Result<(), String> {
        let target = {
            let mut state = self.state.lock();
            if flags.shutdown {
                state.shutdown_requested = true;
            } else {
                state.requested_seq = state.requested_seq.saturating_add(1);
            }
            let target = state.requested_seq;
            self.cv.notify_all();
            if !flags.wait {
                return Ok(());
            }
            target
        };

        let mut state = self.state.lock();
        loop {
            if state.completed_seq >= target {
                return Ok(());
            }
            if state.failed_seq >= target {
                return Err(state
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "checkpoint failed".to_string()));
            }
            if state.worker_exited {
                return Err(state
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "checkpointer exited".to_string()));
            }
            self.cv.wait(&mut state);
        }
    }

    pub fn shutdown_and_join(&self) -> Result<(), String> {
        let result = self.request(CheckpointRequestFlags::shutdown());
        if let Some(handle) = self.handle.lock().take() {
            let _ = handle.join();
        }
        result
    }

    fn worker_main(self: Arc<Self>) {
        let poll_interval = Duration::from_millis(250);
        let mut next_timed_checkpoint = Instant::now() + self.config.checkpoint_timeout;
        loop {
            let (trigger, request_seq) = {
                let mut state = self.state.lock();
                loop {
                    let now = Instant::now();
                    let has_manual_request = state.requested_seq > state.completed_seq.max(state.failed_seq);
                    let timed_due = now >= next_timed_checkpoint;
                    let wal_due = self.wal_due(state.last_checkpoint_lsn);
                    if state.shutdown_requested {
                        break (
                            CheckpointTrigger::Shutdown,
                            state.requested_seq.max(state.completed_seq),
                        );
                    }
                    if has_manual_request {
                        break (CheckpointTrigger::Requested, state.requested_seq);
                    }
                    if wal_due {
                        break (CheckpointTrigger::Requested, state.completed_seq);
                    }
                    if timed_due {
                        break (CheckpointTrigger::Timed, state.completed_seq);
                    }

                    let wait_timeout = next_timed_checkpoint
                        .saturating_duration_since(now)
                        .min(poll_interval);
                    self.cv.wait_for(&mut state, wait_timeout);
                }
            };

            let result = self.perform_checkpoint(trigger);
            let mut state = self.state.lock();
            match result {
                Ok(execution) => {
                    state.last_checkpoint_lsn = execution.end_lsn;
                    state.last_error = None;
                    state.completed_seq = state.completed_seq.max(request_seq);
                }
                Err(err) => {
                    state.last_error = Some(err);
                    state.failed_seq = state.failed_seq.max(request_seq);
                }
            }

            let now = Instant::now();
            next_timed_checkpoint = now + self.config.checkpoint_timeout;
            if matches!(trigger, CheckpointTrigger::Shutdown) {
                state.worker_exited = true;
                self.cv.notify_all();
                break;
            }
            self.cv.notify_all();
        }
    }

    fn wal_due(&self, last_checkpoint_lsn: Lsn) -> bool {
        let Some(wal) = self.wal.as_ref() else {
            return false;
        };
        let max_wal_bytes = self.config.max_wal_size_kb.saturating_mul(1024);
        if max_wal_bytes == 0 {
            return false;
        }
        wal.insert_lsn().saturating_sub(last_checkpoint_lsn) >= max_wal_bytes
    }

    fn perform_checkpoint(&self, trigger: CheckpointTrigger) -> Result<CheckpointExecution, String> {
        let _checkpoint_barrier = self.commit_barrier.begin_checkpoint();
        let write_start = Instant::now();
        let flush_result = self
            .pool
            .checkpoint_flush_all(self.config.fsync)
            .map_err(buffer_error_to_string)?;
        let write_time = write_start.elapsed();

        let sync_start = Instant::now();
        let slru_written = {
            let mut txns = self.txns.write();
            txns.flush_clog().map_err(|err| format!("{err:?}"))?;
            1
        };
        let next_xid = self.txns.read().next_xid();

        let (redo_lsn, end_lsn) = if let Some(wal) = self.wal.as_ref() {
            let redo_lsn = wal.insert_lsn();
            let checkpoint_record = CheckpointRecord { redo_lsn };
            let end_lsn = wal
                .write_checkpoint_record(
                    checkpoint_record,
                    matches!(trigger, CheckpointTrigger::Shutdown),
                )
                .map_err(|err| err.to_string())?;
            wal.flush().map_err(|err| err.to_string())?;
            wal.clear_page_image_tracking();
            (redo_lsn, end_lsn)
        } else {
            (INVALID_LSN, INVALID_LSN)
        };
        let sync_time = sync_start.elapsed();

        if let Some(control_file) = self.control_file.as_ref() {
            control_file
                .update(|control| {
                    control.state = if matches!(trigger, CheckpointTrigger::Shutdown) {
                        ControlFileState::ShutDown
                    } else {
                        ControlFileState::InProduction
                    };
                    control.latest_checkpoint_lsn = end_lsn;
                    control.redo_lsn = redo_lsn;
                    control.next_xid = next_xid;
                    control.full_page_writes = self.config.full_page_writes;
                })
                .map_err(|err| err.to_string())?;
        }

        if let Some(wal) = self.wal.as_ref() {
            wal.recycle_segments(
                redo_lsn,
                self.config.min_wal_size_kb.saturating_mul(1024),
            )
            .map_err(|err| err.to_string())?;
        }

        self.stats.write().record_completed_checkpoint(
            match trigger {
                CheckpointTrigger::Timed => CheckpointCompletionKind::Timed,
                CheckpointTrigger::Requested => CheckpointCompletionKind::Requested,
                CheckpointTrigger::Shutdown => CheckpointCompletionKind::Shutdown,
            },
            write_time,
            sync_time,
            flush_result.buffers_written,
            slru_written,
        );

        Ok(CheckpointExecution { end_lsn })
    }
}

fn buffer_error_to_string(err: BufferError) -> String {
    match err {
        BufferError::UnknownBuffer => "unknown buffer".to_string(),
        BufferError::WrongIoOp => "wrong buffer I/O operation".to_string(),
        BufferError::NoIoInProgress => "no buffer I/O in progress".to_string(),
        BufferError::BufferPinned => "buffer pinned".to_string(),
        BufferError::AllBuffersPinned => "all buffers pinned".to_string(),
        BufferError::InvalidBuffer => "invalid buffer".to_string(),
        BufferError::NotDirty => "buffer not dirty".to_string(),
        BufferError::Storage(message) | BufferError::Wal(message) => message,
    }
}
