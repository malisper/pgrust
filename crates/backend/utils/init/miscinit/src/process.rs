use std::cell::Cell;

use elog::ereport;
use init_small::globals as g;
use types_core::INVALID_PROC_NUMBER;
use types_error::{ErrorLocation, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, FATAL};
use types_storage::latch::LatchHandle;

use crate::{MISCINIT_C, PG_VERSION};

thread_local! {
    // LocalLatchData: one slab slot per backend thread, allocated once, reused.
    static LOCAL_LATCH: Cell<Option<LatchHandle>> = const { Cell::new(None) };
}

fn local_latch() -> LatchHandle {
    if let Some(h) = LOCAL_LATCH.get() {
        return h;
    }
    let h = latch::allocate_local_latch();
    LOCAL_LATCH.set(Some(h));
    h
}

// fork's pid channel is a parameter; identity is never the thread id (M5).
pub fn InitProcessGlobals(my_proc_pid: i32) {
    g::SetMyProcPid(my_proc_pid);
    let ts = timestamp_seams::get_current_timestamp::call();
    g::SetMyStartTimestamp(ts);
    g::SetMyStartTime(timestamptz_to_time_t(ts));

    // C InitProcessGlobals: pg_prng_strong_seed(&pg_global_prng_state), with
    // the pid/start-timestamp mix as the fallback seed. pg_global_prng_state
    // is backend-private in C and thread-local here, so every backend/aux
    // thread (and every wretain task re-init, which re-enters here) must
    // seed its own copy — an unseeded xoroshiro state is the all-zero fixed
    // point and every draw returns 0 (found via hnsw builds drawing the max
    // level for every element).
    let mut seed_bytes = [0u8; 8];
    let rseed = if pg_strong_random::pg_strong_random(&mut seed_bytes) {
        u64::from_ne_bytes(seed_bytes)
    } else {
        ((my_proc_pid as u64) << 48) ^ ((ts as u64) << 16) ^ ((ts as u64) >> 20)
    };
    pg_prng::global_prng(|prng| prng.seed(rseed));
}

const PG_UNIX_EPOCH_OFFSET_SECS: i64 = 946_684_800;

fn timestamptz_to_time_t(t: i64) -> i64 {
    t.div_euclid(1_000_000) + PG_UNIX_EPOCH_OFFSET_SECS
}

// InitStandaloneProcess (miscinit.c:175): the --single / bootstrap process
// environment. Mirrors InitPostmasterChild minus the SIGQUIT unblock ("we
// don't unblock SIGQUIT or provide a default handler for it").
pub fn InitStandaloneProcess(argv0: &str) -> PgResult<()> {
    debug_assert!(!g::IsPostmasterEnvironment());

    crate::SetMyBackendType(types_core::init::BackendType::StandaloneBackend);

    // pgwin32_signal_initialize: WIN32-only, not this build.

    InitProcessGlobals(init_small::globals::process_id() as i32);

    /* Initialize process-local latch support */
    waiteventset::InitializeWaitEventSupport()?;
    InitProcessLocalLatch();
    latch::InitializeLatchWaitSet()?;

    libpq_pqsignal::pqinitmask();
    libpq_pqsignal::block_signals();

    /* Compute paths, no postmaster to inherit from */
    if g::my_exec_path()[0] == 0 {
        let exe = pg_path::find_my_exec(argv0, |m| {
            let _ = ereport(types_error::LOG)
                .errmsg(m)
                .finish(ErrorLocation::new(file!(), line!() as i32, "find_my_exec"));
        })
        .map_err(|_| {
            ereport(FATAL)
                .errmsg(format!("{argv0}: could not locate my own executable path"))
                .into_error()
        })?;
        let mut buf = [0u8; types_core::MAXPGPATH];
        let n = exe.len().min(types_core::MAXPGPATH - 1);
        buf[..n].copy_from_slice(&exe.as_bytes()[..n]);
        g::set_my_exec_path(buf);
    }

    if g::pkglib_path()[0] == 0 {
        let exec_path = c_str_of(&g::my_exec_path());
        let pkglib = pg_path::get_pkglib_path(&exec_path);
        let mut buf = [0u8; types_core::MAXPGPATH];
        let n = pkglib.len().min(types_core::MAXPGPATH - 1);
        buf[..n].copy_from_slice(&pkglib.as_bytes()[..n]);
        g::set_pkglib_path(buf);
    }
    Ok(())
}

fn c_str_of(buf: &[u8]) -> String {
    String::from_utf8_lossy(&buf.iter().copied().take_while(|b| *b != 0).collect::<Vec<u8>>())
        .into_owned()
}

// C's child-init order; process-wide arms are postmaster-signal design.
pub fn InitPostmasterChild(my_proc_pid: i32) -> PgResult<()> {
    g::SetIsUnderPostmaster(true);

    InitProcessGlobals(my_proc_pid);

    libpq_pqsignal::pqinitmask();

    waiteventset::InitializeWaitEventSupport()?;
    InitProcessLocalLatch();
    latch::InitializeLatchWaitSet()?;

    // wasm32: the wasi libc crate exposes no SIG* names; 3 is SIGQUIT in the
    // thread-signal emulation's Linux-numbered space (libpq_pqsignal wasm arm).
    #[cfg(not(target_family = "wasm"))]
    const SIGQUIT: i32 = libc::SIGQUIT;
    #[cfg(target_family = "wasm")]
    const SIGQUIT: i32 = 3;
    libpq_pqsignal::block_sig_delete(SIGQUIT);
    libpq_pqsignal::block_signals();
    Ok(())
}

pub fn InitProcessLocalLatch() {
    let l = local_latch();
    g::SetMyLatch(Some(l));
    latch::InitLatch(l);
}

// C's LocalLatchData dies with the process; a backend THREAD must hand its
// slab slot back or ~4k connections exhaust the slab for the postmaster's
// lifetime. Called only from LocalLatchReleaseGuard at the top of the child
// thread, after all latch use is unwound.
fn release_process_local_latch() {
    let Some(h) = LOCAL_LATCH.take() else { return };
    if g::MyLatch() == Some(h) {
        g::SetMyLatch(None);
    }
    latch::free_local_latch(h);
}

/// Backend-thread teardown for the local latch slot: Drop releases on every
/// exit path — proc_exit unwind and panic alike (SecContextGuard house style).
#[must_use]
pub struct LocalLatchReleaseGuard(());

impl LocalLatchReleaseGuard {
    pub fn new() -> Self {
        Self(())
    }
}

impl Default for LocalLatchReleaseGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for LocalLatchReleaseGuard {
    fn drop(&mut self) {
        release_process_local_latch();
    }
}

pub fn SwitchToSharedLatch() {
    debug_assert_eq!(g::MyLatch(), Some(local_latch()));
    debug_assert_ne!(g::MyProcNumber(), INVALID_PROC_NUMBER);

    let proc_latch = LatchHandle::proc(g::MyProcNumber());
    g::SetMyLatch(Some(proc_latch));

    repoint_fe_be_wait_set(proc_latch);

    // Set the shared latch as the local one might have been set.
    latch::SetLatch(proc_latch);
}

// C: if (FeBeWaitSet) ModifyWaitEvent(..., FeBeWaitSetLatchPos, WL_LATCH_SET,
// MyLatch). Uninstalled seam = a binary with no pqcomm socket half, i.e. no
// FeBeWaitSet; the installed impl no-ops on the unset case itself.
fn repoint_fe_be_wait_set(latch: LatchHandle) {
    if pqcomm_seams::modify_fe_be_wait_set_latch::is_installed() {
        pqcomm_seams::modify_fe_be_wait_set_latch::call(latch)
            .expect("ModifyWaitEvent(FeBeWaitSet, FeBeWaitSetLatchPos)");
    }
}

pub fn SwitchBackToLocalLatch() {
    let l = local_latch();
    debug_assert_ne!(g::MyLatch(), Some(l));
    debug_assert_eq!(g::MyLatch(), Some(LatchHandle::proc(g::MyProcNumber())));

    g::SetMyLatch(Some(l));

    repoint_fe_be_wait_set(l);

    latch::SetLatch(l);
}

pub fn ChangeToDataDir() -> PgResult<()> {
    let data_dir = g::DataDir().expect("ChangeToDataDir: DataDir is set");
    if let Err(e) = std::env::set_current_dir(data_dir) {
        ereport(FATAL)
            .with_saved_errno(e.raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg(format!("could not change directory to \"{data_dir}\": %m"))
            .finish(loc(465, "ChangeToDataDir"))?;
    }
    Ok(())
}

pub fn ValidatePgVersion(path: &str) -> PgResult<()> {
    let my_major = leading_i64(PG_VERSION);
    let full_path = format!("{path}/PG_VERSION");

    let contents = match std::fs::read_to_string(&full_path) {
        Ok(s) => s,
        Err(e) => {
            let errno = e.raw_os_error().unwrap_or(0);
            if errno == libc::ENOENT {
                ereport(FATAL)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("\"{path}\" is not a valid data directory"))
                    .errdetail(format!("File \"{full_path}\" is missing."))
                    .finish(loc(1789, "ValidatePgVersion"))?;
            }
            ereport(FATAL)
                .with_saved_errno(errno)
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{full_path}\": %m"))
                .finish(loc(1795, "ValidatePgVersion"))?;
            return Ok(());
        }
    };

    // fscanf "%63s": first whitespace-delimited token, max 63 bytes.
    let token = contents.split_whitespace().next().unwrap_or("");
    let file_version_string = token.get(..63).unwrap_or(token);
    let starts_numeric = file_version_string
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_digit() || b == b'+' || b == b'-');
    if file_version_string.is_empty() || !starts_numeric {
        ereport(FATAL)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("\"{path}\" is not a valid data directory"))
            .errdetail(format!("File \"{full_path}\" does not contain valid data."))
            .errhint("You might need to initdb.")
            .finish(loc(1805, "ValidatePgVersion"))?;
    }

    if leading_i64(file_version_string) != my_major {
        ereport(FATAL)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("database files are incompatible with server")
            .errdetail(format!(
                "The data directory was initialized by PostgreSQL version {file_version_string}, \
                 which is not compatible with this version {PG_VERSION}."
            ))
            .finish(loc(1816, "ValidatePgVersion"))?;
    }
    Ok(())
}

// strtol(s, NULL, 10).
pub(crate) fn leading_i64(s: &str) -> i64 {
    let bytes = s.trim_start().as_bytes();
    let mut i = 0;
    let mut sign = 1i64;
    if bytes.first().is_some_and(|&b| b == b'+' || b == b'-') {
        if bytes[0] == b'-' {
            sign = -1;
        }
        i = 1;
    }
    let mut val = 0i64;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        val = val.saturating_mul(10).saturating_add((bytes[i] - b'0') as i64);
        i += 1;
    }
    sign.saturating_mul(val)
}

pub(crate) fn loc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new(MISCINIT_C, lineno, funcname)
}
