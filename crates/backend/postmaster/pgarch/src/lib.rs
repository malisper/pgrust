//! pgarch.c: the WAL archiver as a postmaster child thread (walwriter/
//! checkpointer precedents). ArchiveModuleCallbacks is the ArchiveModule enum
//! with the shell module as the only in-core arm; loadable archive_library
//! values are a loud panic.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU32, Ordering::Relaxed};
use std::sync::OnceLock;

use elog::{elog, ereport};
use init_small::globals as g;
use transam_xlog::{
    IsTLHistoryFileName, StatusFilePath, XLogArchivingActive, MAX_XFN_CHARS, MIN_XFN_CHARS,
    VALID_XFN_CHARS, XLOGDIR,
};
use types_core::INVALID_PROC_NUMBER;
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERROR, FATAL, LOG, WARNING,
};
use types_startup::StartupData;
use types_storage::waiteventset::{WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_TIMEOUT};

#[cfg(test)]
mod tests;

const PGARCH_AUTOWAKE_INTERVAL: i64 = 60;
const PGARCH_RESTART_INTERVAL: i64 = 10;
const NUM_ARCHIVE_RETRIES: i32 = 3;
const NUM_ORPHAN_CLEANUP_RETRIES: i32 = 3;
const NUM_FILES_PER_DIRECTORY_SCAN: usize = 64;

const PG_WAIT_ACTIVITY: u32 = 0x0500_0000;
const WAIT_EVENT_ARCHIVER_MAIN: u32 = PG_WAIT_ACTIVITY;

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

struct PgArchData {
    pgprocno: AtomicI32,
    force_dir_scan: AtomicU32,
}

static PGARCH_SHMEM: OnceLock<PgArchData> = OnceLock::new();

fn shmem() -> &'static PgArchData {
    PGARCH_SHMEM.get().expect("PgArch shmem accessed before PgArchShmemInit")
}

pub fn PgArchShmemSize() -> usize {
    core::mem::size_of::<PgArchData>()
}

pub fn PgArchShmemInit() {
    PGARCH_SHMEM
        .set(PgArchData {
            pgprocno: AtomicI32::new(INVALID_PROC_NUMBER),
            force_dir_scan: AtomicU32::new(0),
        })
        .unwrap_or_else(|_| panic!("PgArchShmemInit called twice"));
}

pub fn PgArchShmemResetAfterCrash() {
    let d = shmem();
    d.pgprocno.store(INVALID_PROC_NUMBER, Relaxed);
    d.force_dir_scan.store(0, Relaxed);
    READY_TO_STOP.store(false, Relaxed);
}

static LAST_PGARCH_START_TIME: AtomicI64 = AtomicI64::new(0);

pub fn PgArchCanRestart() -> bool {
    let curtime = time_now();
    if curtime - LAST_PGARCH_START_TIME.load(Relaxed) < PGARCH_RESTART_INTERVAL {
        return false;
    }
    LAST_PGARCH_START_TIME.store(curtime, Relaxed);
    true
}

pub fn PgArchWakeup() {
    // As C: no ProcArrayLock; a stale procno at worst pokes the wrong latch
    // and the relaunched archiver catches up.
    let procno = shmem().pgprocno.load(Relaxed);
    if procno != INVALID_PROC_NUMBER {
        latch::set_latch(&lmgr_proc::ProcGlobal().allProcs[procno as usize].procLatch);
    }
}

pub fn PgArchForceDirScan() {
    shmem().force_dir_scan.store(1, std::sync::atomic::Ordering::SeqCst);
}

// C volatile sig_atomic_t ready_to_stop; per-thread signal delivery may run
// the handler off-thread (checkpointer precedent), hence a process atomic.
static READY_TO_STOP: AtomicBool = AtomicBool::new(false);

fn pgarch_waken_stop() {
    READY_TO_STOP.store(true, Relaxed);
    if let Some(l) = g::MyLatch() {
        latch::SetLatch(l);
    }
}

thread_local! {
    static LAST_SIGTERM_TIME: Cell<i64> = const { Cell::new(0) };
}

fn time_now() -> i64 {
    // DST P2 (contract §1.2): LAST_SIGTERM_TIME on pg_clock::wall_secs().
    pg_clock::wall_secs()
}

fn archive_library() -> String {
    guc_tables::vars::XLogArchiveLibrary.read().unwrap_or_default()
}

fn archive_command_set() -> bool {
    !guc_tables::vars::XLogArchiveCommand.read().unwrap_or_default().is_empty()
}

enum ArchiveModule {
    Shell,
}

impl ArchiveModule {
    fn check_configured(&self) -> Option<String> {
        match self {
            ArchiveModule::Shell => shell_archive::shell_archive_configured(),
        }
    }

    fn archive_file(&self, file: &str, path: &str) -> PgResult<bool> {
        match self {
            ArchiveModule::Shell => shell_archive::shell_archive_file(file, Some(path)),
        }
    }

    fn shutdown(&self) -> PgResult<()> {
        match self {
            ArchiveModule::Shell => shell_archive::shell_archive_shutdown(),
        }
    }
}

fn both_archive_params_error(funcname: &'static str) -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg("both \"archive_command\" and \"archive_library\" set")
        .errdetail("Only one of \"archive_command\", \"archive_library\" may be set.")
        .finish(loc(funcname))
}

fn load_archive_library() -> PgResult<ArchiveModule> {
    let lib = archive_library();
    if !lib.is_empty() && archive_command_set() {
        both_archive_params_error("LoadArchiveLibrary")?;
    }
    if !lib.is_empty() {
        // Loadable archive modules are unported; only the shell
        // archive_command arm exists. This must be an ERROR-channel FATAL,
        // not a Rust panic: the caller fatal_exits (status 1), which the
        // reaper accepts as a normal archiver exit (C CleanupBackend treats
        // 0 and 1 alike for the archiver) — matching C, where a bad
        // archive_library FATALs the archiver and the postmaster relaunches
        // it. A panic here instead maps to WTERMSIG(SIGABRT) at the thread
        // boundary and HandleChildCrash cycles the whole cluster, re-fired
        // on every relaunch (crash loop).
        ereport(FATAL)
            .errmsg(format!(
                "archive_library \"{lib}\": loadable archive modules not ported \
                 (backend-postmaster-pgarch shell arm only)"
            ))
            .errdetail("Unset \"archive_library\" or use \"archive_command\" instead.")
            .finish(loc("LoadArchiveLibrary"))?;
    }
    ipc::before_shmem_exit(pgarch_call_module_shutdown_cb, datum::Datum::null())?;
    Ok(ArchiveModule::Shell)
}

fn pgarch_call_module_shutdown_cb(_code: i32, _arg: datum::Datum) -> PgResult<()> {
    ArchiveModule::Shell.shutdown()
}

fn pgarch_die(_code: i32, _arg: usize) {
    shmem().pgprocno.store(INVALID_PROC_NUMBER, Relaxed);
}

fn fatal_exit(e: &PgError) -> ! {
    elog::emit_error_report_for(e);
    ipc::proc_exit(1, g::MyProcPid())
}

pub fn PgArchiverMain(startup_data: &StartupData) -> ! {
    debug_assert!(matches!(startup_data, StartupData::None));

    miscinit::SetMyBackendType(types_core::BackendType::Archiver);
    if let Err(e) = auxprocess::AuxiliaryProcessMainCommon() {
        fatal_exit(&e);
    }

    {
        use procsignal::ThreadSignalHandler::{Ignore, Simple};
        procsignal::pqsignal_thread(procsignal::signums::SIGHUP, Simple(interrupt::SignalHandlerForConfigReload));
        procsignal::pqsignal_thread(procsignal::signums::SIGINT, Ignore);
        procsignal::pqsignal_thread(
            procsignal::signums::SIGTERM,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
        procsignal::pqsignal_thread(procsignal::signums::SIGALRM, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGPIPE, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGUSR2, Simple(pgarch_waken_stop));
    }

    libpq_pqsignal::unblock_signals();

    debug_assert!(XLogArchivingActive());

    READY_TO_STOP.store(false, Relaxed);
    LAST_SIGTERM_TIME.set(0);

    ipc::on_shmem_exit(pgarch_die, 0);

    shmem().pgprocno.store(g::MyProcNumber(), Relaxed);

    let mut arch_files = ArchFilesState::new();

    let module = match load_archive_library() {
        Ok(m) => m,
        Err(e) => fatal_exit(&e),
    };

    if let Err(e) = pgarch_MainLoop(&mut arch_files, &module) {
        fatal_exit(&e);
    }

    ipc::proc_exit(0, g::MyProcPid())
}

fn pgarch_MainLoop(af: &mut ArchFilesState, module: &ArchiveModule) -> PgResult<()> {
    loop {
        let mut time_to_stop;
        if let Some(l) = g::MyLatch() {
            latch::ResetLatch(l);
        }

        time_to_stop = READY_TO_STOP.load(Relaxed);

        ProcessPgArchInterrupts()?;

        if interrupt::ShutdownRequestPending() {
            let curtime = time_now();
            if LAST_SIGTERM_TIME.get() == 0 {
                LAST_SIGTERM_TIME.set(curtime);
            } else if curtime - LAST_SIGTERM_TIME.get() >= 60 {
                return Ok(());
            }
        }

        pgarch_ArchiverCopyLoop(af, module)?;

        if !time_to_stop {
            let rc = latch::WaitLatch(
                g::MyLatch(),
                WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                PGARCH_AUTOWAKE_INTERVAL * 1000,
                WAIT_EVENT_ARCHIVER_MAIN,
            )?;
            if rc & WL_POSTMASTER_DEATH != 0 {
                time_to_stop = true;
            }
        }

        if time_to_stop {
            return Ok(());
        }
    }
}

fn pgarch_ArchiverCopyLoop(af: &mut ArchFilesState, module: &ArchiveModule) -> PgResult<()> {
    af.files_size = 0;

    while let Some(xlog) = pgarch_readyXlog(af)? {
        let mut failures = 0;
        let mut failures_orphan = 0;

        loop {
            // C also bails if !PostmasterIsAlive(); one address space makes
            // postmaster death unobservable from a live thread.
            if interrupt::ShutdownRequestPending() {
                return Ok(());
            }

            ProcessPgArchInterrupts()?;

            if let Some(errdetail) = module.check_configured() {
                ereport(WARNING)
                    .errmsg("\"archive_mode\" enabled, yet archiving is not configured")
                    .errdetail_internal(errdetail)
                    .finish(loc("pgarch_ArchiverCopyLoop"))?;
                return Ok(());
            }

            let pathname = format!("{XLOGDIR}/{xlog}");
            let missing = match std::fs::metadata(&pathname) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
                _ => false,
            };
            if missing {
                let xlogready = StatusFilePath(&xlog, ".ready");
                if std::fs::remove_file(&xlogready).is_ok() {
                    ereport(WARNING)
                        .errmsg(format!("removed orphan archive status file \"{xlogready}\""))
                        .finish(loc("pgarch_ArchiverCopyLoop"))?;
                    break;
                }
                failures_orphan += 1;
                if failures_orphan >= NUM_ORPHAN_CLEANUP_RETRIES {
                    ereport(WARNING)
                        .errmsg(format!(
                            "removal of orphan archive status file \"{xlogready}\" failed too many times, will try again later"
                        ))
                        .finish(loc("pgarch_ArchiverCopyLoop"))?;
                    return Ok(());
                }
                std::thread::sleep(std::time::Duration::from_secs(1));
                continue;
            }

            if pgarch_archiveXlog(&xlog, module)? {
                pgarch_archiveDone(&xlog)?;
                pgstat::archiver::pgstat_report_archiver(&xlog, false);
                break;
            }

            pgstat::archiver::pgstat_report_archiver(&xlog, true);
            failures += 1;
            if failures >= NUM_ARCHIVE_RETRIES {
                ereport(WARNING)
                    .errmsg(format!(
                        "archiving write-ahead log file \"{xlog}\" failed too many times, will try again later"
                    ))
                    .finish(loc("pgarch_ArchiverCopyLoop"))?;
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }
    Ok(())
}

/// pgarch_archiveXlog: the C sigsetjmp block catching ERROR from the archive
/// callback; FATAL/PANIC propagate (C: proc_exit path).
fn pgarch_archiveXlog(xlog: &str, module: &ArchiveModule) -> PgResult<bool> {
    let pathname = format!("{XLOGDIR}/{xlog}");

    if ps_status_seams::set_ps_display::is_installed() {
        ps_status_seams::set_ps_display::call(&format!("archiving {xlog}"));
    }

    let ret = match module.archive_file(xlog, &pathname) {
        Ok(ok) => ok,
        Err(e) if e.level < FATAL => {
            archive_error_cleanup(&e);
            false
        }
        Err(e) => return Err(e),
    };

    if ps_status_seams::set_ps_display::is_installed() {
        let msg =
            if ret { format!("last was {xlog}") } else { format!("failed on {xlog}") };
        ps_status_seams::set_ps_display::call(&msg);
    }
    Ok(ret)
}

fn archive_error_cleanup(err: &PgError) {
    g::HoldInterrupts();
    elog::emit_error_report_for(err);

    if timeout_seams::disable_all_timeouts::is_installed() {
        let _ = timeout_seams::disable_all_timeouts::call(false);
    }
    let _ = lwlock::LWLockReleaseAll();
    if condition_variable_seams::condition_variable_cancel_sleep::is_installed() {
        condition_variable_seams::condition_variable_cancel_sleep::call();
    }
    waitevent_seams::pgstat_report_wait_end::call();
    if aio_seams::pgaio_error_cleanup::is_installed() {
        aio_seams::pgaio_error_cleanup::call();
    }
    let _ = resowner::ReleaseAuxProcessResources(false);
    let _ = fd::AtEOXact_Files(false);
    dynahash::AtEOXact_HashTables(false);

    elog::FlushErrorState();
    g::ResumeInterrupts();
}

struct ArchFilesState {
    heap_size: usize,
    heap: [u16; NUM_FILES_PER_DIRECTORY_SCAN],
    files_size: usize,
    files: [u16; NUM_FILES_PER_DIRECTORY_SCAN],
    name_len: [u8; NUM_FILES_PER_DIRECTORY_SCAN],
    names: [[u8; MAX_XFN_CHARS]; NUM_FILES_PER_DIRECTORY_SCAN],
}

impl ArchFilesState {
    fn new() -> Self {
        ArchFilesState {
            heap_size: 0,
            heap: [0; NUM_FILES_PER_DIRECTORY_SCAN],
            files_size: 0,
            files: [0; NUM_FILES_PER_DIRECTORY_SCAN],
            name_len: [0; NUM_FILES_PER_DIRECTORY_SCAN],
            names: [[0; MAX_XFN_CHARS]; NUM_FILES_PER_DIRECTORY_SCAN],
        }
    }

    fn name(&self, slot: u16) -> &str {
        let s = slot as usize;
        // Slot bytes come from a &str dirent name.
        std::str::from_utf8(&self.names[s][..self.name_len[s] as usize]).unwrap()
    }

    fn set_name(&mut self, slot: u16, name: &str) {
        let s = slot as usize;
        self.names[s][..name.len()].copy_from_slice(name.as_bytes());
        self.name_len[s] = name.len() as u8;
    }

    fn cmp_slots(&self, a: u16, b: u16) -> std::cmp::Ordering {
        ready_file_cmp(self.name(a), self.name(b))
    }

    fn sift_down(&mut self, mut i: usize) {
        loop {
            let l = 2 * i + 1;
            let r = 2 * i + 2;
            let mut largest = i;
            if l < self.heap_size
                && self.cmp_slots(self.heap[l], self.heap[largest]).is_gt()
            {
                largest = l;
            }
            if r < self.heap_size
                && self.cmp_slots(self.heap[r], self.heap[largest]).is_gt()
            {
                largest = r;
            }
            if largest == i {
                return;
            }
            self.heap.swap(i, largest);
            i = largest;
        }
    }

    fn sift_up(&mut self, mut i: usize) {
        while i > 0 {
            let parent = (i - 1) / 2;
            if self.cmp_slots(self.heap[i], self.heap[parent]).is_gt() {
                self.heap.swap(i, parent);
                i = parent;
            } else {
                return;
            }
        }
    }

    fn heap_build(&mut self) {
        for i in (0..self.heap_size / 2).rev() {
            self.sift_down(i);
        }
    }

    fn heap_first(&self) -> u16 {
        self.heap[0]
    }

    fn heap_remove_first(&mut self) -> u16 {
        let first = self.heap[0];
        self.heap_size -= 1;
        self.heap[0] = self.heap[self.heap_size];
        self.sift_down(0);
        first
    }
}

/// ready_file_comparator inverted into Ordering terms: Greater = lower
/// archival priority (the max-heap keeps the worst candidate on top).
fn ready_file_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    let a_history = IsTLHistoryFileName(a);
    let b_history = IsTLHistoryFileName(b);
    if a_history != b_history {
        return if a_history { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater };
    }
    a.cmp(b)
}

fn pgarch_readyXlog(af: &mut ArchFilesState) -> PgResult<Option<String>> {
    if shmem().force_dir_scan.swap(0, std::sync::atomic::Ordering::SeqCst) == 1 {
        af.files_size = 0;
    }

    while af.files_size > 0 {
        af.files_size -= 1;
        let arch_file = af.name(af.files[af.files_size]).to_string();
        let status_file = StatusFilePath(&arch_file, ".ready");
        match std::fs::metadata(&status_file) {
            Ok(_) => return Ok(Some(arch_file)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                ereport(ERROR)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!("could not stat file \"{status_file}\": %m"))
                    .finish(loc("pgarch_readyXlog"))?;
            }
        }
    }

    af.heap_size = 0;

    let status_dir = format!("{XLOGDIR}/archive_status");
    let rldir = fd::AllocateDir(&status_dir)?;
    while let Some(rlde) = fd::ReadDir(rldir, &status_dir)? {
        let d_name = &rlde.d_name;
        let Some(basenamelen) = d_name.len().checked_sub(".ready".len()) else { continue };
        if !(MIN_XFN_CHARS..=MAX_XFN_CHARS).contains(&basenamelen) {
            continue;
        }
        if !d_name[..basenamelen].bytes().all(|c| VALID_XFN_CHARS.as_bytes().contains(&c)) {
            continue;
        }
        if &d_name[basenamelen..] != ".ready" {
            continue;
        }
        let basename = &d_name[..basenamelen];

        if af.heap_size < NUM_FILES_PER_DIRECTORY_SCAN {
            let slot = af.heap_size as u16;
            af.set_name(slot, basename);
            af.heap[af.heap_size] = slot;
            af.heap_size += 1;
            if af.heap_size == NUM_FILES_PER_DIRECTORY_SCAN {
                af.heap_build();
            }
        } else if ready_file_cmp(af.name(af.heap_first()), basename).is_gt() {
            let slot = af.heap_remove_first();
            af.set_name(slot, basename);
            af.heap[af.heap_size] = slot;
            af.heap_size += 1;
            af.sift_up(af.heap_size - 1);
        }
    }
    fd::FreeDir(rldir)?;

    if af.heap_size == 0 {
        return Ok(None);
    }

    if af.heap_size < NUM_FILES_PER_DIRECTORY_SCAN {
        af.heap_build();
    }

    af.files_size = af.heap_size;
    for i in 0..af.files_size {
        af.files[i] = af.heap_remove_first();
    }

    af.files_size -= 1;
    Ok(Some(af.name(af.files[af.files_size]).to_string()))
}

fn pgarch_archiveDone(xlog: &str) -> PgResult<()> {
    let rlogready = StatusFilePath(xlog, ".ready");
    let rlogdone = StatusFilePath(xlog, ".done");
    if let Err(e) = std::fs::rename(&rlogready, &rlogdone) {
        ereport(WARNING)
            .with_saved_errno(e.raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg(format!("could not rename file \"{rlogready}\" to \"{rlogdone}\": %m"))
            .finish(loc("pgarch_archiveDone"))?;
    }
    Ok(())
}

fn ProcessPgArchInterrupts() -> PgResult<()> {
    if procsignal_seams::proc_signal_barrier_pending::call() {
        procsignal_seams::process_proc_signal_barrier::call()?;
    }

    if mcxt_seams::log_memory_context_pending::is_installed()
        && mcxt_seams::log_memory_context_pending::call()
    {
        mcxt_seams::process_log_memory_context_interrupt::call()?;
    }

    if interrupt::ConfigReloadPending() {
        let archive_lib = archive_library();

        interrupt::SetConfigReloadPending(false);
        guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;

        if !archive_library().is_empty() && archive_command_set() {
            both_archive_params_error("ProcessPgArchInterrupts")?;
        }

        if archive_library() != archive_lib {
            elog(
                LOG,
                "restarting archiver process because value of \"archive_library\" was changed",
            )?;
            ipc::proc_exit(0, g::MyProcPid());
        }
    }
    Ok(())
}

pub fn init_seams() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::XLogArchiveLibrary.install(GucVarAccessors {
        get: || XLOG_ARCHIVE_LIBRARY.get().map(str::to_string),
        set: |v| XLOG_ARCHIVE_LIBRARY.set(v.map(|s| &*s.leak())),
    });
}

// XLogArchiveLibrary (pgarch.c): leaked-&'static-str cell, reloads boot-rare.
thread_local! {
    static XLOG_ARCHIVE_LIBRARY: Cell<Option<&'static str>> = const { Cell::new(Some("")) };
}
