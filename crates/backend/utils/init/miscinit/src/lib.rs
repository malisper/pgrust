//! miscinit.c: processing-mode/backend-type globals, the user-id /
//! security-restriction state machine, the LocalLatchData home,
//! ClientConnectionInfo serialization, and the lock-file interlock.
//! Deferred (owners unported): has_rolreplication, the system_user() SQL
//! wrapper.

#![allow(non_snake_case)]

use std::cell::Cell;

use types_core::{uaReject, BackendType, ProcessingMode, UserAuth};
use types_error::{PgError, PgResult};

mod datadir;
mod guard;
mod lockfile;
mod preload;
mod process;
mod userid;

pub use datadir::{checkDataDir, make_absolute_path, SetDataDir};
pub use guard::SecContextGuard;
pub use preload::{
    process_shared_preload_libraries, process_shared_preload_libraries_done,
    process_shared_preload_libraries_in_progress,
    process_shmem_requests,
};
pub use lockfile::{
    AddToDataDirLockFile, CreateDataDirLockFile, CreateSocketLockFile, RecheckDataDirLockFile,
    TouchSocketLockFiles, UnlinkLockFiles,
};
pub use process::{
    ChangeToDataDir, InitPostmasterChild, InitProcessGlobals, InitProcessLocalLatch,
    InitStandaloneProcess, LocalLatchReleaseGuard, SwitchBackToLocalLatch, SwitchToSharedLatch,
    ValidatePgVersion,
};
pub use userid::*;

pub(crate) const MISCINIT_C: &str = "src/backend/utils/init/miscinit.c";
pub(crate) const PG_VERSION: &str = "18.3";

thread_local! {
    static MODE: Cell<ProcessingMode> = const { Cell::new(ProcessingMode::InitProcessing) };
    static MY_BACKEND_TYPE: Cell<BackendType> = const { Cell::new(BackendType::Invalid) };
    static IGNORE_SYSTEM_INDEXES: Cell<bool> = const { Cell::new(false) };
    // authn_id: C TopMemoryContext char*, set at most once per thread; leaked.
    static CLIENT_AUTHN_ID: Cell<Option<&'static str>> = const { Cell::new(None) };
    static CLIENT_AUTH_METHOD: Cell<UserAuth> = const { Cell::new(uaReject) };
}

pub fn GetProcessingMode() -> ProcessingMode {
    MODE.get()
}

pub fn SetProcessingMode(mode: ProcessingMode) {
    MODE.set(mode);
}

pub fn IsBootstrapProcessingMode() -> bool {
    MODE.get() == ProcessingMode::BootstrapProcessing
}

pub fn IsInitProcessingMode() -> bool {
    MODE.get() == ProcessingMode::InitProcessing
}

pub fn IsNormalProcessingMode() -> bool {
    MODE.get() == ProcessingMode::NormalProcessing
}

pub fn GetMyBackendType() -> BackendType {
    MY_BACKEND_TYPE.get()
}

pub fn SetMyBackendType(backend_type: BackendType) {
    MY_BACKEND_TYPE.set(backend_type);
}

pub fn IgnoreSystemIndexes() -> bool {
    IGNORE_SYSTEM_INDEXES.get()
}

pub fn SetIgnoreSystemIndexes(ignore: bool) {
    IGNORE_SYSTEM_INDEXES.set(ignore);
}

pub fn GetBackendTypeDesc(backend_type: BackendType) -> &'static str {
    match backend_type {
        BackendType::Invalid => "not initialized",
        BackendType::Archiver => "archiver",
        BackendType::AutovacLauncher => "autovacuum launcher",
        BackendType::AutovacWorker => "autovacuum worker",
        BackendType::Backend => "client backend",
        BackendType::DeadEndBackend => "dead-end client backend",
        BackendType::BgWorker => "background worker",
        BackendType::BgWriter => "background writer",
        BackendType::Checkpointer => "checkpointer",
        BackendType::IoWorker => "io worker",
        BackendType::Logger => "logger",
        BackendType::SlotsyncWorker => "slotsync worker",
        BackendType::StandaloneBackend => "standalone backend",
        BackendType::Startup => "startup",
        BackendType::WalReceiver => "walreceiver",
        BackendType::WalSender => "walsender",
        BackendType::WalSummarizer => "walsummarizer",
        BackendType::WalWriter => "walwriter",
    }
}

// Once per process; the global itself is globals.c-owned.
pub fn SetDatabasePath(path: &str) {
    debug_assert!(init_small::globals::DatabasePath().is_none());
    init_small::globals::SetDatabasePath(path);
}

// SerializedClientConnectionInfo: int32 authn_id_len, then UserAuth (int width).
const SERIALIZED_HEADER_LEN: usize = 8;

pub fn set_client_connection_info(authn_id: Option<&str>, auth_method: UserAuth) {
    CLIENT_AUTHN_ID.set(authn_id.map(|s| &*String::from(s).leak()));
    CLIENT_AUTH_METHOD.set(auth_method);
}

pub fn client_connection_info() -> (Option<&'static str>, UserAuth) {
    (CLIENT_AUTHN_ID.get(), CLIENT_AUTH_METHOD.get())
}

pub fn EstimateClientConnectionInfoSpace() -> usize {
    match CLIENT_AUTHN_ID.get() {
        Some(id) => SERIALIZED_HEADER_LEN + id.len() + 1,
        None => SERIALIZED_HEADER_LEN,
    }
}

pub fn SerializeClientConnectionInfo(start_address: &mut [u8]) {
    let authn_id = CLIENT_AUTHN_ID.get();
    let authn_id_len: i32 = authn_id.map_or(-1, |id| id.len() as i32);

    assert!(start_address.len() >= EstimateClientConnectionInfoSpace());
    start_address[..4].copy_from_slice(&authn_id_len.to_ne_bytes());
    start_address[4..8].copy_from_slice(&CLIENT_AUTH_METHOD.get().to_ne_bytes());

    if let Some(id) = authn_id {
        let body = SERIALIZED_HEADER_LEN + id.len();
        start_address[SERIALIZED_HEADER_LEN..body].copy_from_slice(id.as_bytes());
        start_address[body] = 0; // NUL terminator eases deserialization
    }
}

pub fn RestoreClientConnectionInfo(conninfo: &[u8]) -> PgResult<()> {
    if conninfo.len() < SERIALIZED_HEADER_LEN {
        return Err(PgError::error("client connection info buffer is too small").into());
    }
    let authn_id_len = i32::from_ne_bytes(conninfo[..4].try_into().unwrap());
    let auth_method = UserAuth::from_ne_bytes(conninfo[4..8].try_into().unwrap());

    let authn_id = if authn_id_len >= 0 {
        let end = SERIALIZED_HEADER_LEN + authn_id_len as usize;
        if conninfo.len() < end + 1 {
            return Err(PgError::error("client connection info buffer is too small").into());
        }
        let text = std::str::from_utf8(&conninfo[SERIALIZED_HEADER_LEN..end])
            .map_err(|_| PgError::error("invalid serialized client authn_id"))?;
        Some(text)
    } else {
        None
    };
    set_client_connection_info(authn_id, auth_method);
    Ok(())
}

/// Install every `miscinit_seams` declaration this crate bodies.
pub fn init_seams() {
    use miscinit_seams as s;

    s::get_user_id::set(GetUserId);
    s::get_session_user_id::set(GetSessionUserId);
    s::get_user_id_and_sec_context::set(GetUserIdAndSecContext);
    s::set_user_id_and_sec_context::set(SetUserIdAndSecContext);
    s::get_user_name_from_id::set(GetUserNameFromId);
    s::is_bootstrap_processing_mode::set(IsBootstrapProcessingMode);
    // Recovery-only DatabasePath poke/clear (inval.c), sans the one-shot assert.
    s::set_database_path::set(init_small::globals::SetDatabasePath);
    s::clear_database_path::set(init_small::globals::ClearDatabasePath);
    s::switch_to_shared_latch::set(SwitchToSharedLatch);
    s::switch_back_to_local_latch::set(SwitchBackToLocalLatch);
    s::create_socket_lock_file::set(CreateSocketLockFile);
    s::check_data_dir::set(checkDataDir);
    s::initialize_session_user_id::set(InitializeSessionUserId);
    s::process_shared_preload_libraries::set(process_shared_preload_libraries);
    s::process_preload_contrib::set(preload::process_preload_contrib);
    s::process_session_preload_libraries::set(preload::process_session_preload_libraries);
    s::process_shmem_requests::set(process_shmem_requests);
    preload::install_preload_guc_vars();
}

#[cfg(test)]
mod tests;
