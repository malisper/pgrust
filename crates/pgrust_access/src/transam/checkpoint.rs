use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Condvar, Mutex, RwLock};
use pgrust_nodes::datetime::{TimestampTzADT, USECS_PER_DAY, USECS_PER_SEC};

use crate::transam::xact::TransactionManager;
use crate::transam::xlog::{INVALID_LSN, Lsn, WalWriter};
use crate::transam::{ControlFileState, ControlFileStore};
use pgrust_storage::buffer::storage_backend::SmgrStorageBackend;
use pgrust_storage::buffer::{BufferPool, Error as BufferError};
use pgrust_storage::sync::SyncQueue;

const KB_PER_MB: u64 = 1024;
const KB_PER_GB: u64 = 1024 * 1024;
const UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS: i64 = 10_957;

#[derive(Debug, Clone, PartialEq)]
pub struct CheckpointConfig {
    pub checkpoint_timeout: Duration,
    pub checkpoint_completion_target: f64,
    pub checkpoint_warning: Duration,
    pub max_wal_size_kb: u64,
    pub min_wal_size_kb: u64,
    pub fsync: bool,
    pub full_page_writes: bool,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_timeout: Duration::from_secs(300),
            checkpoint_completion_target: 0.9,
            checkpoint_warning: Duration::from_secs(30),
            max_wal_size_kb: KB_PER_GB,
            min_wal_size_kb: 80 * KB_PER_MB,
            fsync: true,
            full_page_writes: true,
        }
    }
}

impl CheckpointConfig {
    pub fn load_from_data_dir(base_dir: &Path) -> Result<Self, String> {
        let mut config = Self::default();
        apply_checkpoint_config_file(&mut config, &base_dir.join("postgresql.conf"))?;
        apply_checkpoint_config_file(&mut config, &base_dir.join("postgresql.auto.conf"))?;
        Ok(config)
    }

    pub fn value_for_show(&self, name: &str) -> Option<String> {
        match name {
            "checkpoint_timeout" => Some(format_duration(self.checkpoint_timeout)),
            "checkpoint_completion_target" => Some(self.checkpoint_completion_target.to_string()),
            "checkpoint_warning" => Some(format_duration(self.checkpoint_warning)),
            "max_wal_size" => Some(format_wal_size_kb(self.max_wal_size_kb)),
            "min_wal_size" => Some(format_wal_size_kb(self.min_wal_size_kb)),
            "fsync" => Some(format_bool(self.fsync)),
            "full_page_writes" => Some(format_bool(self.full_page_writes)),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CheckpointStatsSnapshot {
    pub num_timed: u64,
    pub num_requested: u64,
    pub num_done: u64,
    pub write_time_ms: f64,
    pub sync_time_ms: f64,
    pub buffers_written: u64,
    pub slru_written: u64,
    pub stats_reset: TimestampTzADT,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointCompletionKind {
    Timed,
    Requested,
    EndOfRecovery,
    Shutdown,
}

impl Default for CheckpointStatsSnapshot {
    fn default() -> Self {
        Self {
            num_timed: 0,
            num_requested: 0,
            num_done: 0,
            write_time_ms: 0.0,
            sync_time_ms: 0.0,
            buffers_written: 0,
            slru_written: 0,
            stats_reset: TimestampTzADT(current_postgres_timestamp_usecs()),
        }
    }
}

impl CheckpointStatsSnapshot {
    pub fn record_completed_checkpoint(
        &mut self,
        kind: CheckpointCompletionKind,
        write_time: Duration,
        sync_time: Duration,
        buffers_written: u64,
        slru_written: u64,
    ) {
        match kind {
            CheckpointCompletionKind::Timed => {
                self.num_timed = self.num_timed.saturating_add(1);
            }
            CheckpointCompletionKind::Requested => {
                self.num_requested = self.num_requested.saturating_add(1);
            }
            CheckpointCompletionKind::EndOfRecovery | CheckpointCompletionKind::Shutdown => {}
        }
        self.num_done = self.num_done.saturating_add(1);
        self.write_time_ms += write_time.as_secs_f64() * 1000.0;
        self.sync_time_ms += sync_time.as_secs_f64() * 1000.0;
        self.buffers_written = self.buffers_written.saturating_add(buffers_written);
        self.slru_written = self.slru_written.saturating_add(slru_written);
    }

    pub fn record_manual_checkpoint(&mut self) {
        self.record_completed_checkpoint(
            CheckpointCompletionKind::Requested,
            Duration::ZERO,
            Duration::ZERO,
            0,
            0,
        );
    }
}

pub(crate) fn current_postgres_timestamp_usecs() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            let unix_usecs =
                duration.as_secs() as i64 * USECS_PER_SEC + duration.subsec_micros() as i64;
            unix_usecs - UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS * USECS_PER_DAY
        }
        Err(err) => {
            let duration = err.duration();
            let unix_usecs =
                duration.as_secs() as i64 * USECS_PER_SEC + duration.subsec_micros() as i64;
            -unix_usecs - UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS * USECS_PER_DAY
        }
    }
}

fn normalize_guc_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

fn is_checkpoint_guc(name: &str) -> bool {
    matches!(
        name,
        "checkpoint_timeout"
            | "checkpoint_completion_target"
            | "checkpoint_warning"
            | "max_wal_size"
            | "min_wal_size"
            | "fsync"
            | "full_page_writes"
    )
}

fn apply_checkpoint_config_file(config: &mut CheckpointConfig, path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let text = fs::read_to_string(path)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    for (index, raw_line) in text.lines().enumerate() {
        let line = strip_config_comment(raw_line).trim();
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        let name = normalize_guc_name(name);
        if !is_checkpoint_guc(&name) {
            continue;
        }
        let value = unquote_config_value(value.trim());
        apply_checkpoint_setting(config, &name, value)
            .map_err(|message| format!("{}:{}: {message}", path.display(), index + 1))?;
    }
    Ok(())
}

fn strip_config_comment(line: &str) -> &str {
    let mut in_single = false;
    let mut in_double = false;
    for (idx, ch) in line.char_indices() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '#' if !in_single && !in_double => return &line[..idx],
            _ => {}
        }
    }
    line
}

fn unquote_config_value(value: &str) -> &str {
    let bytes = value.as_bytes();
    if bytes.len() >= 2
        && ((bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\'')
            || (bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"'))
    {
        &value[1..value.len() - 1]
    } else {
        value
    }
}

fn apply_checkpoint_setting(
    config: &mut CheckpointConfig,
    name: &str,
    value: &str,
) -> Result<(), String> {
    match name {
        "checkpoint_timeout" => {
            config.checkpoint_timeout = parse_duration_setting(value)?;
        }
        "checkpoint_completion_target" => {
            config.checkpoint_completion_target = value
                .trim()
                .parse::<f64>()
                .map_err(|_| format!("invalid value for {name}: {value}"))?;
        }
        "checkpoint_warning" => {
            config.checkpoint_warning = parse_duration_setting(value)?;
        }
        "max_wal_size" => {
            config.max_wal_size_kb = parse_size_kb(value)?;
        }
        "min_wal_size" => {
            config.min_wal_size_kb = parse_size_kb(value)?;
        }
        "fsync" => {
            config.fsync = parse_bool_setting(value)?;
        }
        "full_page_writes" => {
            config.full_page_writes = parse_bool_setting(value)?;
        }
        _ => {}
    }
    Ok(())
}

fn parse_bool_setting(value: &str) -> Result<bool, String> {
    match normalize_guc_name(value).as_str() {
        "on" | "true" | "yes" | "1" => Ok(true),
        "off" | "false" | "no" | "0" => Ok(false),
        _ => Err(format!("invalid boolean value: {value}")),
    }
}

fn parse_duration_setting(value: &str) -> Result<Duration, String> {
    let normalized = normalize_guc_name(value);
    let (amount, unit) = split_numeric_suffix(&normalized)?;
    let amount = amount
        .parse::<u64>()
        .map_err(|_| format!("invalid duration value: {value}"))?;
    let seconds = match unit {
        "" | "s" => amount,
        "ms" => 0,
        "min" => amount.saturating_mul(60),
        "h" => amount.saturating_mul(60 * 60),
        "d" => amount.saturating_mul(60 * 60 * 24),
        _ => return Err(format!("invalid duration unit in {value}")),
    };
    if unit == "ms" {
        Ok(Duration::from_millis(amount))
    } else {
        Ok(Duration::from_secs(seconds))
    }
}

fn parse_size_kb(value: &str) -> Result<u64, String> {
    let normalized = normalize_guc_name(value);
    let (amount, unit) = split_numeric_suffix(&normalized)?;
    let amount = amount
        .parse::<u64>()
        .map_err(|_| format!("invalid size value: {value}"))?;
    match unit {
        "" | "kb" => Ok(amount),
        "mb" => Ok(amount.saturating_mul(KB_PER_MB)),
        "gb" => Ok(amount.saturating_mul(KB_PER_GB)),
        _ => Err(format!("invalid size unit in {value}")),
    }
}

fn split_numeric_suffix(value: &str) -> Result<(&str, &str), String> {
    let idx = value
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(value.len());
    if idx == 0 {
        return Err(format!("missing numeric value: {value}"));
    }
    Ok((&value[..idx], value[idx..].trim()))
}

fn format_bool(value: bool) -> String {
    if value { "on" } else { "off" }.to_string()
}

fn format_duration(value: Duration) -> String {
    let seconds = value.as_secs();
    if seconds % 60 == 0 && seconds >= 60 {
        format!("{}min", seconds / 60)
    } else {
        format!("{seconds}s")
    }
}

fn format_wal_size_kb(value: u64) -> String {
    if value % KB_PER_GB == 0 && value >= KB_PER_GB {
        format!("{}GB", value / KB_PER_GB)
    } else if value % KB_PER_MB == 0 && value >= KB_PER_MB {
        format!("{}MB", value / KB_PER_MB)
    } else {
        format!("{value}kB")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointRequestFlags {
    pub wait: bool,
    pub immediate: bool,
    pub force: bool,
    pub shutdown: bool,
    pub end_of_recovery: bool,
}

impl CheckpointRequestFlags {
    pub const fn sql() -> Self {
        Self {
            wait: true,
            immediate: true,
            force: true,
            shutdown: false,
            end_of_recovery: false,
        }
    }

    pub const fn shutdown() -> Self {
        Self {
            wait: true,
            immediate: true,
            force: true,
            shutdown: true,
            end_of_recovery: false,
        }
    }

    pub const fn end_of_recovery() -> Self {
        Self {
            wait: true,
            immediate: true,
            force: true,
            shutdown: false,
            end_of_recovery: true,
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
    EndOfRecovery,
    Shutdown,
}

struct CheckpointerState {
    requested_seq: u64,
    completed_seq: u64,
    failed_seq: u64,
    last_error: Option<String>,
    stop_requested: bool,
    shutdown_requested: bool,
    end_of_recovery_requested: bool,
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
    sync_queue: Arc<SyncQueue>,
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
        sync_queue: Arc<SyncQueue>,
        commit_barrier: Arc<CheckpointCommitBarrier>,
    ) -> Arc<Self> {
        let initial_lsn = wal
            .as_ref()
            .map(|wal| wal.insert_lsn())
            .unwrap_or(INVALID_LSN);
        let checkpointer = Arc::new(Self {
            pool,
            wal,
            txns,
            control_file,
            config,
            stats,
            sync_queue,
            commit_barrier,
            state: Mutex::new(CheckpointerState {
                requested_seq: 0,
                completed_seq: 0,
                failed_seq: 0,
                last_error: None,
                stop_requested: false,
                shutdown_requested: false,
                end_of_recovery_requested: false,
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
                if flags.end_of_recovery {
                    state.end_of_recovery_requested = true;
                }
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

    pub fn stop_and_join(&self) {
        {
            let mut state = self.state.lock();
            state.stop_requested = true;
            self.cv.notify_all();
        }
        if let Some(handle) = self.handle.lock().take() {
            let _ = handle.join();
        }
    }

    fn worker_main(self: Arc<Self>) {
        let poll_interval = Duration::from_millis(250);
        let mut next_timed_checkpoint = Instant::now() + self.config.checkpoint_timeout;
        loop {
            let (trigger, request_seq) = {
                let mut state = self.state.lock();
                loop {
                    let now = Instant::now();
                    let has_end_of_recovery_request = state.end_of_recovery_requested
                        && state.requested_seq > state.completed_seq.max(state.failed_seq);
                    let has_manual_request =
                        state.requested_seq > state.completed_seq.max(state.failed_seq);
                    let timed_due = now >= next_timed_checkpoint;
                    let wal_due = self.wal_due(state.last_checkpoint_lsn);
                    if state.stop_requested {
                        state.worker_exited = true;
                        self.cv.notify_all();
                        return;
                    }
                    if state.shutdown_requested {
                        break (
                            CheckpointTrigger::Shutdown,
                            state.requested_seq.max(state.completed_seq),
                        );
                    }
                    if has_end_of_recovery_request {
                        break (CheckpointTrigger::EndOfRecovery, state.requested_seq);
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
                    if matches!(trigger, CheckpointTrigger::EndOfRecovery) {
                        state.end_of_recovery_requested = false;
                    }
                    state.completed_seq = state.completed_seq.max(request_seq);
                }
                Err(err) => {
                    if matches!(trigger, CheckpointTrigger::EndOfRecovery) {
                        state.end_of_recovery_requested = false;
                    }
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

    fn perform_checkpoint(
        &self,
        trigger: CheckpointTrigger,
    ) -> Result<CheckpointExecution, String> {
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
        self.pool
            .with_storage_mut(|storage| self.sync_queue.process_pending_syncs(&mut storage.smgr))
            .map_err(|err| format!("{err}"))?;
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
            wal.recycle_segments(redo_lsn, self.config.min_wal_size_kb.saturating_mul(1024))
                .map_err(|err| err.to_string())?;
        }

        self.stats.write().record_completed_checkpoint(
            match trigger {
                CheckpointTrigger::Timed => CheckpointCompletionKind::Timed,
                CheckpointTrigger::Requested => CheckpointCompletionKind::Requested,
                CheckpointTrigger::EndOfRecovery => CheckpointCompletionKind::EndOfRecovery,
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
