//! syslogger.c boot surface: the GUC homes and the logrotate signal-file
//! probes. The collector process itself is unported (SysLogger_Start panics
//! in the postmaster; write_syslogger_file stays an uninstalled seam).

#![allow(non_snake_case)]

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::RwLock;

use guc_tables::{vars, GucVarAccessors};

const LOGROTATE_SIGNAL_FILE: &str = "logrotate";

static LOGGING_COLLECTOR: AtomicBool = AtomicBool::new(false);
static LOG_ROTATION_AGE: AtomicI32 = AtomicI32::new(24 * 60);
static LOG_ROTATION_SIZE: AtomicI32 = AtomicI32::new(10 * 1024);
static LOG_TRUNCATE_ON_ROTATION: AtomicBool = AtomicBool::new(false);
static LOG_FILE_MODE: AtomicI32 = AtomicI32::new(0o600);
static LOG_DIRECTORY: RwLock<Option<String>> = RwLock::new(None);
static LOG_FILENAME: RwLock<Option<String>> = RwLock::new(None);

pub fn Logging_collector() -> bool {
    LOGGING_COLLECTOR.load(Ordering::Relaxed)
}

pub fn CheckLogrotateSignal() -> bool {
    std::fs::metadata(LOGROTATE_SIGNAL_FILE).is_ok()
}

pub fn RemoveLogrotateSignalFiles() {
    let _ = std::fs::remove_file(LOGROTATE_SIGNAL_FILE);
}

fn string_get(cell: &'static RwLock<Option<String>>, boot: &'static str) -> Option<String> {
    match &*cell.read().unwrap() {
        Some(s) => Some(s.clone()),
        None => Some(boot.to_string()),
    }
}

pub fn init_seams() {
    vars::Logging_collector.install(GucVarAccessors {
        get: Logging_collector,
        set: |v| LOGGING_COLLECTOR.store(v, Ordering::Relaxed),
    });
    vars::Log_RotationAge.install(GucVarAccessors {
        get: || LOG_ROTATION_AGE.load(Ordering::Relaxed),
        set: |v| LOG_ROTATION_AGE.store(v, Ordering::Relaxed),
    });
    vars::Log_RotationSize.install(GucVarAccessors {
        get: || LOG_ROTATION_SIZE.load(Ordering::Relaxed),
        set: |v| LOG_ROTATION_SIZE.store(v, Ordering::Relaxed),
    });
    vars::Log_truncate_on_rotation.install(GucVarAccessors {
        get: || LOG_TRUNCATE_ON_ROTATION.load(Ordering::Relaxed),
        set: |v| LOG_TRUNCATE_ON_ROTATION.store(v, Ordering::Relaxed),
    });
    vars::Log_file_mode.install(GucVarAccessors {
        get: || LOG_FILE_MODE.load(Ordering::Relaxed),
        set: |v| LOG_FILE_MODE.store(v, Ordering::Relaxed),
    });
    vars::Log_directory.install(GucVarAccessors {
        get: || string_get(&LOG_DIRECTORY, "log"),
        set: |v| *LOG_DIRECTORY.write().unwrap() = v,
    });
    vars::Log_filename.install(GucVarAccessors {
        get: || string_get(&LOG_FILENAME, "postgresql-%Y-%m-%d_%H%M%S.log"),
        set: |v| *LOG_FILENAME.write().unwrap() = v,
    });
    syslogger_seams::check_logrotate_signal::set(CheckLogrotateSignal);
    syslogger_seams::remove_logrotate_signal_files::set(RemoveLogrotateSignalFiles);
}
