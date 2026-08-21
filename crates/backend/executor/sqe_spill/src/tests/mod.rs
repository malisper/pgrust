//! The M2-B acceptance battery (charter `lanev3-m2-chunks.md` §5, M2-B
//! row):
//!
//! - [`layout`] — size_of pins on everything that prices engagement, ref
//!   encoding laws, header fail-closed validation.
//! - [`pages`] — page views + the SWIZZLE METAMORPHIC suites
//!   (write/unload/reload/probe equivalence under permuted unload points).
//! - [`pool`] — budget/eviction/floor/jumbo laws, reload validation,
//!   in-place rewrite identity.
//! - [`streams`] — writer/reader events, commit watermark, extents,
//!   segment splits, CROSS-THREAD write→open-by-name→read.
//! - [`teardown`] — delete-all on the five enumerated paths (normal,
//!   ERROR, cancel, FATAL, crash-restart).
//! - [`limits`] — temp_file_limit pinned (error identity + accounting),
//!   the release-effective fail-closed admission probe.
//! - [`records`] — golden byte contracts vs the donors (join batch
//!   records; the DistinctSet record formats).
//! - [`byref`] — the byref enumeration belt + Send/Sync witnesses.
//!
//! Harness: the fd test pattern (fd/src/tests.rs) — scratch datadir,
//! seam installation, per-thread `InitFileAccess` + temp-file access. The
//! CWD mutex serializes tests because fd paths are datadir-relative.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, MutexGuard, Once};

mod byref;
mod layout;
mod limits;
mod pages;
mod pool;
mod records;
mod streams;
mod teardown;

static SETUP: Once = Once::new();
static CWD: Mutex<()> = Mutex::new(());

/// Install the process-wide seams once (the fd tests' setup set plus
/// resowner, which SpillSet file registration needs on every path).
pub(crate) fn setup_process() {
    SETUP.call_once(|| {
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        resowner::init_seams();

        xact_seams::get_current_sub_transaction_id::set(|| 1);
        postgres_seams::check_for_interrupts::set(|| Ok(()));
        aio_seams::pgaio_closing_fd::set(|_| {});
        aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        pgstat_seams::pgstat_report_tempfile::set(|_| {});
        ipc_seams::on_shmem_exit::set(|_cb, _arg| {});
        ipc_seams::before_shmem_exit::set(|_cb, _arg| Ok(()));
    });
}

/// Arm THIS thread's fd substrate: VFD cache + temp-file access + a live
/// resource owner (the registration belt every temp create/open takes).
pub(crate) fn setup_thread() {
    setup_process();
    fd::InitFileAccess();
    fd::InitTemporaryFileAccess().unwrap();
    let owner =
        resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "sqe_spill-test")
            .unwrap();
    resowner_seams::set_current_resource_owner::call(owner);
}

/// A fresh scratch datadir with the `base/pgsql_tmp` skeleton, entered as
/// CWD under the serialization lock.
pub(crate) fn scratch_datadir(tag: &str) -> (String, MutexGuard<'static, ()>) {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir()
        .join(format!("pgrust_sqe_spill_{}_{tag}_{n}", std::process::id()))
        .to_str()
        .unwrap()
        .to_owned();
    let guard = CWD.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    std::fs::create_dir_all(format!("{dir}/base/pgsql_tmp")).unwrap();
    // The reaper walks pg_tblspc too (RemovePgTempFiles) — a real datadir
    // always has it.
    std::fs::create_dir_all(format!("{dir}/pg_tblspc")).unwrap();
    std::env::set_current_dir(&dir).unwrap();
    (dir, guard)
}

/// The fileset temp dir this process's sets land in (datadir-relative).
pub(crate) const TMP_DIR: &str = "base/pgsql_tmp";

/// List the entries of `base/pgsql_tmp` (empty when absent).
pub(crate) fn tmp_entries() -> Vec<String> {
    match std::fs::read_dir(TMP_DIR) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Standard test rig: one SpillSet in a fresh datadir.
pub(crate) fn rig(tag: &str) -> (std::sync::Arc<crate::SpillSet>, String, MutexGuard<'static, ()>) {
    setup_thread();
    let (dir, guard) = scratch_datadir(tag);
    let set = crate::SpillSet::create().unwrap();
    (set, dir, guard)
}
